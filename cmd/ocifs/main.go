package main

import (
	"log"

	"github.com/greatliontech/ocifs/internal/fuseinterface"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
}

var rootFlags = &rootCmdFlags{}

func main() {
	// bind command-line flags
	rootCmd.Flags().StringVarP(&rootFlags.MountPoint, "mountpoint", "m", "", "Directory to mount OCI image")
	rootCmd.MarkFlagRequired("mountpoint")
	rootCmd.Flags().StringVarP(&rootFlags.ImageRef, "image", "i", "", "Image to mount")
	rootCmd.MarkFlagRequired("image")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute root command: %v", err)
	}
}

func rootCmdRunE(cmd *cobra.Command, args []string) error {
	// Initialize GitFS
	root, err := fuseinterface.NewOciFS(rootFlags.ImageRef)
	if err != nil {
		log.Fatalf("Failed to initialize OciFS: %v", err)
	}

	// Create a FUSE server
	server, err := fs.Mount(rootFlags.MountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: false, // Set to true for debugging
		},
	})
	if err != nil {
		log.Fatalf("Failed to mount OciFS: %v", err)
	}

	// Serve the filesystem until unmounted
	server.Serve()

	return nil
}
