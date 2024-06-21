package main

import (
	"log"
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
	ExtraDirs  []string
	WorkDir    string
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
		log.Fatalf("Failed to execute root command: %v", err)
	}
}

func rootCmdRunE(cmd *cobra.Command, args []string) error {
	opts := []ocifs.Option{
		ocifs.WithWorkDir(rootFlags.WorkDir),
	}
	if len(rootFlags.ExtraDirs) > 0 {
		opts = append(opts, ocifs.WithExtraDirs(rootFlags.ExtraDirs))
	}

	ofs, err := ocifs.New(opts...)
	if err != nil {
		return err
	}

	h, err := ofs.Pull(rootFlags.ImageRef)
	if err != nil {
		return err
	}

	// Create a FUSE server
	server, err := ofs.Mount(h, rootFlags.MountPoint)
	if err != nil {
		log.Fatalf("Failed to mount OciFS: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.Unmount()
	}()

	// Serve the filesystem until unmounted
	server.Wait()

	return nil
}
