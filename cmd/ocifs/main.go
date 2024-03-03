package main

import (
	"log"
	"os"
	"os/signal"
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
	ofs, err := ocifs.New()
	if err != nil {
		return err
	}

	// Create a FUSE server
	server, err := ofs.Mount(rootFlags.ImageRef, rootFlags.MountPoint)
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
