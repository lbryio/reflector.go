package cmd

import (
	"encoding/hex"
	"os"
	"time"

	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/stream"

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
		peer.NewStore(peer.StoreOpts{Address: addr}),
		store.NewDiskBlobStore("/tmp/lbry_downloaded_blobs", 1000, 2),
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

	filename := sd.SuggestedFileName
	if filename == "" {
		filename = "stream_" + time.Now().Format("20060102_150405")
	}

	f, err := os.Create(wd + "/" + filename)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(sd.BlobInfos)-1; i++ {
		b, err := s.Get(hex.EncodeToString(sd.BlobInfos[i].BlobHash))
		if err != nil {
			log.Fatal(err)
		}

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
