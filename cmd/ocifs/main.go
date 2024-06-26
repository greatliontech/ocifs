package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/greatliontech/ocifs"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ocifs",
	Short: "mounts an OCI image as a filesystem",
	RunE:  rootCmdRunE,
}

type rootCmdFlags struct {
	MountPoint string
	ImageRef   string
	WorkDir    string
	ExtraDirs  []string
}

var rootFlags = &rootCmdFlags{}

func main() {
	// bind command-line flags
	rootCmd.Flags().StringVarP(&rootFlags.MountPoint, "mountpoint", "m", "", "Directory to mount OCI image")
	rootCmd.MarkFlagRequired("mountpoint")
	rootCmd.Flags().StringVarP(&rootFlags.ImageRef, "image", "i", "", "Image to mount")
	rootCmd.MarkFlagRequired("image")
	rootCmd.Flags().StringVarP(&rootFlags.WorkDir, "workdir", "w", filepath.Join(os.TempDir(), "ocifs"), "Work directory")
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
	opts := []ocifs.Option{
		ocifs.WithWorkDir(rootFlags.WorkDir),
		ocifs.WithEnableDefaultKeychain(),
	}
	if len(rootFlags.ExtraDirs) > 0 {
		opts = append(opts, ocifs.WithExtraDirs(rootFlags.ExtraDirs))
	}

	ofs, err := ocifs.New(opts...)
	if err != nil {
		return err
	}

	// Mount the OCI image
	im, err := ofs.Mount(rootFlags.ImageRef, ocifs.MountWithTargetPath(rootFlags.MountPoint))
	if err != nil {
		log.Fatalf("Failed to mount OciFS: %v", err)
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
			slog.Error("Failed to unmount", "error", err)
		}
	}()

	// Serve the filesystem until unmounted
	im.Wait()

	return nil
}
