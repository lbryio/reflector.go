package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/lbryio/reflector.go/wallet"

	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "resolve ADDRESS:PORT URL",
		Short: "Resolve a URL",
		Args:  cobra.ExactArgs(2),
		Run:   resolveCmd,
	}
	rootCmd.AddCommand(cmd)
}

func resolveCmd(cmd *cobra.Command, args []string) {
	addr := args[0]
	url := args[1]

	node := wallet.NewNode()
	defer node.Shutdown()
	err := node.Connect([]string{addr}, nil)
	checkErr(err)

	claim, _, err := node.ResolveToClaim(url)
	checkErr(err)

	jsonClaim, err := json.MarshalIndent(claim, "", "  ")
	checkErr(err)

	fmt.Println(string(jsonClaim))
}
