package cmd

import (
	"encoding/hex"
	"fmt"

	"github.com/lbryio/lbry.go/v2/schema/claim"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/jsonpb"
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

	m := jsonpb.Marshaler{Indent: "  "}

	if stream := c.Claim.GetStream(); stream != nil {
		json, err := m.MarshalToString(stream)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(json)
		fmt.Printf("SD hash as hex: %s\n", hex.EncodeToString(stream.GetSource().GetSdHash()))
	} else if channel := c.Claim.GetChannel(); channel != nil {
		json, err := m.MarshalToString(channel)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(json)
	} else if repost := c.Claim.GetRepost(); repost != nil {
		json, err := m.MarshalToString(repost)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(json)
	} else {
		spew.Dump(c)
	}
}
