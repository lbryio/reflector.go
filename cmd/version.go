package cmd

import (
	"fmt"
	"time"

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
	fmt.Printf("version %s (%s)\n", meta.Version, meta.BuildTime.Format(time.RFC3339))
}
