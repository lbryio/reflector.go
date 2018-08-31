package cmd

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "test",
		Short: "Test things",
		Run:   testCmd,
	}
	rootCmd.AddCommand(cmd)
}

func testCmd(cmd *cobra.Command, args []string) {
	spew.Dump(reflector.BlockedSdHashes())
}
