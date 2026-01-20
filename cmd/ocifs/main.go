package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/greatliontech/ocifs"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ocifs imageRef",
	Short: "mounts an OCI image as a filesystem",
	RunE:  rootCmdRunE,
	Args:  cobra.ExactArgs(1),
}

type rootCmdFlags struct {
	MountPoint  string
	WorkDir     string
	ExtraDirs   []string
	WritableDir string
}

var rootFlags = &rootCmdFlags{}

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish]",
	Short: "Generate shell completion script",
	Long: `Generate shell completion script for ocifs.

To load completions:

Bash:
  $ source <(ocifs completion bash)
  # Or install permanently:
  $ ocifs completion bash > ~/.local/share/bash-completion/completions/ocifs

Zsh:
  $ source <(ocifs completion zsh)
  # Or install permanently:
  $ ocifs completion zsh > ~/.local/share/zsh/site-functions/_ocifs

Fish:
  $ ocifs completion fish | source
  # Or install permanently:
  $ ocifs completion fish > ~/.config/fish/completions/ocifs.fish
`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"bash", "zsh", "fish"},
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return rootCmd.GenBashCompletion(os.Stdout)
		case "zsh":
			return rootCmd.GenZshCompletion(os.Stdout)
		case "fish":
			return rootCmd.GenFishCompletion(os.Stdout, true)
		default:
			return nil
		}
	},
}

func main() {
	// initialize logging
	initLogging()

	// add subcommands
	rootCmd.AddCommand(completionCmd)

	// bind command-line flags
	rootCmd.Flags().StringVarP(&rootFlags.MountPoint, "mountpoint", "m", "", "Directory to mount OCI image")
	rootCmd.Flags().StringVarP(&rootFlags.WorkDir, "workdir", "w", filepath.Join(os.TempDir(), "ocifs"), "Work directory")
	rootCmd.Flags().StringVarP(&rootFlags.WritableDir, "writedir", "W", "", "Directory to use for writable layer (enables read-write mode)")
	extraDirs := rootCmd.Flags().StringSliceP("extra-dirs", "e", nil, "Extra directories to include in the mount")
	if extraDirs != nil {
		rootFlags.ExtraDirs = *extraDirs
	}

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Failed to execute", "error", err)
		os.Exit(1)
	}
}

func rootCmdRunE(cmd *cobra.Command, args []string) error {
	imageRef := args[0]

	opts := []ocifs.Option{
		ocifs.WithWorkDir(rootFlags.WorkDir),
		ocifs.WithEnableDefaultKeychain(),
	}

	ofs, err := ocifs.New(opts...)
	if err != nil {
		return err
	}

	mountOpts := []ocifs.MountOption{
		ocifs.MountWithExtraDirs(rootFlags.ExtraDirs),
		ocifs.MountWithWritableDir(rootFlags.WritableDir),
		ocifs.MountWithTargetPath(rootFlags.MountPoint),
	}

	// Mount the OCI image
	im, err := ofs.Mount(imageRef, mountOpts...)
	if err != nil {
		log.Fatalf("mount failed: %v", err)
	}

	sigtermHandler := func() chan os.Signal {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		return c
	}
	go func() {
		for {
			<-sigtermHandler()
			err := im.Unmount()
			if err == nil {
				break
			}
			slog.Error("unmount failed", "error", err)
		}
	}()

	// Serve the filesystem until unmounted
	if err := im.Wait(); err != nil {
		slog.Error("wait failed", "error", err)
	}

	return nil
}

// initLogging configures the global slog logger based on an environment variable.
func initLogging() {
	// Default to logging only errors.
	logLevel := slog.LevelError

	// Check the environment variable for a different log level.
	switch strings.ToLower(os.Getenv("OCIFS_LOG_LEVEL")) {
	case "info":
		logLevel = slog.LevelInfo
	case "debug":
		logLevel = slog.LevelDebug
	}

	// Create a new logger with the chosen level and set it as the default.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)
}
