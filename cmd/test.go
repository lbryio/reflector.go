package cmd

import (
	log "github.com/sirupsen/logrus"
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
	log.Println("test :-)")
}
