package cmd

import (
	"encoding/hex"
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

	output, err := node.Resolve(url)
	checkErr(err)

	claim, err := node.GetClaimInTx(hex.EncodeToString(rev(output.GetTxHash())), int(output.GetNout()))
	checkErr(err)

	jsonClaim, err := json.MarshalIndent(claim, "", "  ")
	checkErr(err)

	fmt.Println(string(jsonClaim))
}

func rev(b []byte) []byte {
	r := make([]byte, len(b))
	for left, right := 0, len(b)-1; left < right; left, right = left+1, right-1 {
		r[left], r[right] = b[right], b[left]
	}
	return r
}
