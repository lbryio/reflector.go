package cmd

import (
	"io/ioutil"
	"os"

	"github.com/lbryio/lbry.go/stream"

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

	s, err := c.GetStream(sdHash)
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}

	var sd stream.SDBlob
	err = sd.FromBlob(s[0])
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}

	log.Printf("Downloading %d blobs for %s", len(sd.BlobInfos)-1, sd.SuggestedFileName)

	data, err := s.Data()
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}

	filename := wd + "/" + sd.SuggestedFileName
	err = ioutil.WriteFile(filename, data, 0644)
	if err != nil {
		log.Error(errors.FullTrace(err))
		return
	}

	log.Printf("Wrote %d bytes to %s\n", len(data), filename)
}
