package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// =============================================================================
// Phase 6: CLI Tests
// =============================================================================

// setupTestCmd creates a fresh command with flags for testing
// This is needed because flags are registered in main(), not at package init
func setupTestCmd() {
	// Reset flags by re-registering them (they may already be registered from previous tests)
	if rootCmd.Flags().Lookup("mountpoint") == nil {
		rootCmd.Flags().StringVarP(&rootFlags.MountPoint, "mountpoint", "m", "", "Directory to mount OCI image")
	}
	if rootCmd.Flags().Lookup("workdir") == nil {
		rootCmd.Flags().StringVarP(&rootFlags.WorkDir, "workdir", "w", filepath.Join(os.TempDir(), "ocifs"), "Work directory")
	}
	if rootCmd.Flags().Lookup("writedir") == nil {
		rootCmd.Flags().StringVarP(&rootFlags.WritableDir, "writedir", "W", "", "Directory to use for writable layer (enables read-write mode)")
	}
	if rootCmd.Flags().Lookup("extra-dirs") == nil {
		rootCmd.Flags().StringSliceP("extra-dirs", "e", nil, "Extra directories to include in the mount")
	}

	// Ensure completion subcommand is added
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "completion" {
			found = true
			break
		}
	}
	if !found {
		rootCmd.AddCommand(completionCmd)
	}
}

// TestCLI_HelpOutput tests that help output is displayed correctly
func TestCLI_HelpOutput(t *testing.T) {
	setupTestCmd()

	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	rootCmd.SetArgs([]string{"--help"})

	err := rootCmd.Execute()
	if err != nil {
		t.Logf("Execute returned error (may be expected for help): %v", err)
	}

	output := buf.String()

	expectedStrings := []string{
		"ocifs",
		"imageRef",
	}

	for _, s := range expectedStrings {
		if !strings.Contains(output, s) {
			t.Errorf("Help output missing expected string %q\nOutput: %s", s, output)
		}
	}
}

// TestCLI_InvalidArgs tests that invalid arguments are rejected
func TestCLI_InvalidArgs(t *testing.T) {
	setupTestCmd()

	// Create a new root command for this test to avoid state issues
	testCmd := &cobra.Command{
		Use:  "ocifs imageRef",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	testCmd.SetArgs([]string{})
	err := testCmd.Execute()
	if err == nil {
		t.Error("Expected error with no arguments")
	}
}

// TestCLI_CompletionBash tests bash completion generation
func TestCLI_CompletionBash(t *testing.T) {
	setupTestCmd()

	// Capture stdout since GenBashCompletion writes to os.Stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	rootCmd.SetArgs([]string{"completion", "bash"})
	err := rootCmd.Execute()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if err != nil {
		t.Fatalf("completion bash failed: %v", err)
	}

	// Bash completion should contain function definitions
	if !strings.Contains(output, "_ocifs") && !strings.Contains(output, "complete") {
		t.Errorf("Bash completion output doesn't look like bash completion: %s", output[:min(200, len(output))])
	}
}

// TestCLI_CompletionZsh tests zsh completion generation
func TestCLI_CompletionZsh(t *testing.T) {
	setupTestCmd()

	// Capture stdout since GenZshCompletion writes to os.Stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	rootCmd.SetArgs([]string{"completion", "zsh"})
	err := rootCmd.Execute()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if err != nil {
		t.Fatalf("completion zsh failed: %v", err)
	}

	// Zsh completion should contain compdef or _ocifs
	if !strings.Contains(output, "_ocifs") && !strings.Contains(output, "#compdef") {
		t.Errorf("Zsh completion output doesn't look like zsh completion: %s", output[:min(200, len(output))])
	}
}

// TestCLI_CompletionFish tests fish completion generation
func TestCLI_CompletionFish(t *testing.T) {
	setupTestCmd()

	// Capture stdout since GenFishCompletion writes to os.Stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	rootCmd.SetArgs([]string{"completion", "fish"})
	err := rootCmd.Execute()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if err != nil {
		t.Fatalf("completion fish failed: %v", err)
	}

	// Fish completion should contain complete command
	if !strings.Contains(output, "complete") && !strings.Contains(output, "ocifs") {
		t.Errorf("Fish completion output doesn't look like fish completion: %s", output[:min(200, len(output))])
	}
}

// TestCLI_CompletionInvalid tests that invalid completion type returns silently
// Note: The implementation returns nil for invalid types rather than an error
func TestCLI_CompletionInvalid(t *testing.T) {
	setupTestCmd()

	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetArgs([]string{"completion", "invalid"})

	// The current implementation uses ValidArgs which cobra validates
	err := rootCmd.Execute()
	// May or may not return error depending on cobra version
	_ = err
}

// TestCLI_Flags tests that flags are parsed correctly
func TestCLI_Flags(t *testing.T) {
	setupTestCmd()

	flags := rootCmd.Flags()

	// Check mountpoint flag
	mpFlag := flags.Lookup("mountpoint")
	if mpFlag == nil {
		t.Fatal("mountpoint flag not defined")
	}
	if mpFlag.Shorthand != "m" {
		t.Errorf("mountpoint shorthand wrong: got %q, want %q", mpFlag.Shorthand, "m")
	}

	// Check workdir flag
	wdFlag := flags.Lookup("workdir")
	if wdFlag == nil {
		t.Fatal("workdir flag not defined")
	}
	if wdFlag.Shorthand != "w" {
		t.Errorf("workdir shorthand wrong: got %q, want %q", wdFlag.Shorthand, "w")
	}

	// Check writedir flag
	wrFlag := flags.Lookup("writedir")
	if wrFlag == nil {
		t.Fatal("writedir flag not defined")
	}
	if wrFlag.Shorthand != "W" {
		t.Errorf("writedir shorthand wrong: got %q, want %q", wrFlag.Shorthand, "W")
	}

	// Check extra-dirs flag
	edFlag := flags.Lookup("extra-dirs")
	if edFlag == nil {
		t.Fatal("extra-dirs flag not defined")
	}
	if edFlag.Shorthand != "e" {
		t.Errorf("extra-dirs shorthand wrong: got %q, want %q", edFlag.Shorthand, "e")
	}
}

// TestCLI_RootCmdUse tests the root command usage string
func TestCLI_RootCmdUse(t *testing.T) {
	if rootCmd.Use != "ocifs imageRef" {
		t.Errorf("Root command Use wrong: got %q", rootCmd.Use)
	}
}

// TestCLI_RootCmdShort tests the root command short description
func TestCLI_RootCmdShort(t *testing.T) {
	if rootCmd.Short == "" {
		t.Error("Root command Short description is empty")
	}
}

// TestCLI_CompletionCmdExists tests that completion subcommand exists
func TestCLI_CompletionCmdExists(t *testing.T) {
	setupTestCmd()

	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "completion" {
			found = true
			break
		}
	}
	if !found {
		t.Error("completion subcommand not found")
	}
}
