package cmd

import (
	"encoding/hex"
	"os"

	"github.com/lbryio/reflector.go/peer"

	"github.com/lbryio/lbry.go/stream"

	"github.com/lbryio/reflector.go/store"

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

	s := store.NewCachingBlobStore(
		peer.NewStore(addr),
		store.NewDiskBlobStore("/tmp/lbry_downloaded_blobs", 2),
	)

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	var sd stream.SDBlob

	sdb, err := s.Get(sdHash)
	if err != nil {
		log.Fatal(err)
	}

	err = sd.FromBlob(sdb)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(wd + "/" + sd.SuggestedFileName)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(sd.BlobInfos)-1; i++ {
		bb, err := s.Get(hex.EncodeToString(sd.BlobInfos[i].BlobHash))
		if err != nil {
			log.Fatal(err)
		}
		b := stream.Blob(bb)

		data, err := b.Plaintext(sd.Key, sd.BlobInfos[i].IV)
		if err != nil {
			log.Fatal(err)
		}

		_, err = f.Write(data)
		if err != nil {
			log.Fatal(err)
		}
	}
}
