package cmd

import (
	"github.com/lbryio/lbryschema.go/claim"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "decode VALUE",
		Short: "Decode a claim value",
		Args:  cobra.ExactArgs(1),
		Run:   decodeCmd,
	}
	rootCmd.AddCommand(cmd)
}

func decodeCmd(cmd *cobra.Command, args []string) {
	c, err := claim.DecodeClaimHex(args[0], "")
	if err != nil {
		log.Fatal(err)
	}

	if stream := c.Claim.GetStream(); stream != nil {
		spew.Dump(stream)
	} else if channel := c.Claim.GetChannel(); channel != nil {
		spew.Dump(channel)
	} else if repost := c.Claim.GetRepost(); channel != nil {
		spew.Dump(repost)
	} else {
		spew.Dump(c)
	}
}
