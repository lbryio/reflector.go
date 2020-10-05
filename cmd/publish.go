package cmd

import (
	"fmt"

	"github.com/lbryio/reflector.go/publish"

	"github.com/lbryio/lbry.go/v2/lbrycrd"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "publish FILE",
		Short: "Publish a file",
		Args:  cobra.ExactArgs(1),
		Run:   publishCmd,
	}
	cmd.Flags().String("name", "", "Claim name")
	cmd.Flags().String("title", "", "Title of the content")
	cmd.Flags().String("description", "", "Description of the content")
	cmd.Flags().String("author", "", "Content author")
	cmd.Flags().String("tags", "", "Comma-separated list of tags")
	cmd.Flags().Int64("release-time", 0, "original public release of content, seconds since UNIX epoch")
	rootCmd.AddCommand(cmd)
}

func publishCmd(cmd *cobra.Command, args []string) {
	var err error

	claimName := mustGetFlagString(cmd, "name")
	if claimName == "" {
		log.Errorln("--name required")
		return
	}

	path := args[0]

	client, err := lbrycrd.NewWithDefaultURL(nil)
	checkErr(err)

	tx, txid, err := publish.Publish(
		client,
		path,
		claimName,
		"bSzpgkTnAoiT2YAhUShPpfpajPESfNXVTu",
		publish.Details{
			Title:       mustGetFlagString(cmd, "title"),
			Description: mustGetFlagString(cmd, "description"),
			Author:      mustGetFlagString(cmd, "author"),
			Tags:        nil,
			ReleaseTime: mustGetFlagInt64(cmd, "release-time"),
		},
		"reflector.lbry.com:5566",
	)
	checkErr(err)

	decoded, err := publish.Decode(client, tx)
	checkErr(err)

	fmt.Printf("TX: %s\n\n", decoded)
	fmt.Printf("TXID: %s\n", txid.String())
}
