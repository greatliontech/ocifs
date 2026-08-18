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

	gcCmd.Flags().StringVarP(&gcFlags.WorkDir, "workdir", "w", filepath.Join(os.TempDir(), "ocifs"), "Work directory")
	gcCmd.Flags().BoolVar(&gcFlags.IgnoreGrace, "ignore-grace", false, "Collect unreachable content regardless of age")
	rootCmd.AddCommand(gcCmd)

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Failed to execute", "error", err)
		os.Exit(1)
	}
}

type gcCmdFlags struct {
	WorkDir     string
	IgnoreGrace bool
}

var gcFlags = &gcCmdFlags{}

// gcCmd is the explicit-collection verb (api.md REQ-api-gc): the
// same engine as the library surface, reporting what was collected
// and what could not be judged.
var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "collect unreachable store content",
	RunE: func(cmd *cobra.Command, args []string) error {
		ofs, err := ocifs.New(ocifs.WithWorkDir(gcFlags.WorkDir))
		if err != nil {
			return err
		}
		defer ofs.Close()
		var opts []ocifs.GCOption
		if gcFlags.IgnoreGrace {
			opts = append(opts, ocifs.GCIgnoreGrace())
		}
		res, err := ofs.GC(cmd.Context(), opts...)
		if err != nil {
			return err
		}
		for _, h := range res.CollectedBlobs {
			cmd.Printf("collected blob %s\n", h)
		}
		for _, p := range res.CollectedPaths {
			cmd.Printf("collected path %s\n", p)
		}
		for _, id := range res.ReclaimedMounts {
			cmd.Printf("reclaimed dead mount %s\n", id)
		}
		for _, id := range res.ForeignVersionRows {
			cmd.Printf("live foreign-version mount row %s halted image-tier collection (a matching ocifs version's sweep resolves it)\n", id)
		}
		return nil
	},
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

	// Register the signal handler before mounting: a TERM landing in
	// the mount window parks in the buffered channel instead of
	// killing the process with the mount half-served.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	// Mount the OCI image
	im, err := ofs.Mount(rootFlags.ImageRef, ocifs.MountWithTargetPath(rootFlags.MountPoint))
	if err != nil {
		log.Fatalf("Failed to mount OciFS: %v", err)
	}

	go func() {
		for range sigs {
			if err := im.Unmount(); err == nil {
				return
			} else {
				slog.Error("Failed to unmount", "error", err)
			}
		}
	}()

	// Serve the filesystem until unmounted
	im.Wait()

	return nil
}
