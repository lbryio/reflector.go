package cmd

import (
	"os"

	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/extras/errors"
	"github.com/lbryio/reflector.go/peer"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "getstream ADDRESS:PORT SDHASH",
		Short: "Get a stream from a reflector server",
		Args:  cobra.ExactArgs(2),
		Run:   getStreamCmd,
	}
	rootCmd.AddCommand(cmd)
}

func getStreamCmd(cmd *cobra.Command, args []string) {
	addr := args[0]
	sdHash := args[1]

	c := peer.Client{}
	err := c.Connect(addr)
	if err != nil {
		log.Fatal("error connecting client to server: ", err)
	}

	cache := store.NewFileBlobStore("/tmp/lbry_downloaded_blobs")

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	err = c.WriteStream(sdHash, wd, cache)
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}
}
