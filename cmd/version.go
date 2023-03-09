package cmd

import (
	"fmt"

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
	fmt.Println(meta.FullName())
}
