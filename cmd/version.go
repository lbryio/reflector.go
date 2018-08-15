package cmd

import (
	"log"

	"github.com/lbryio/reflector.go/meta"

	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run:   versionCmd,
	}
	rootCmd.AddCommand(cmd)
}

func versionCmd(cmd *cobra.Command, args []string) {
	log.Printf("version %s\n", meta.Version)
}
