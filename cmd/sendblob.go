package cmd

import (
	"crypto/rand"
	"os"

	"github.com/lbryio/reflector.go/reflector"

	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "sendblob ADDRESS:PORT [PATH]",
		Short: "Send a random blob to a reflector server",
		Args:  cobra.RangeArgs(1, 2),
		Run:   sendBlobCmd,
	}
	rootCmd.AddCommand(cmd)
}

func sendBlobCmd(cmd *cobra.Command, args []string) {
	addr := args[0]
	var path string
	if len(args) >= 2 {
		path = args[1]
	}

	c := reflector.Client{}
	err := c.Connect(addr)
	if err != nil {
		log.Fatal("error connecting client to server: ", err)
	}

	if path == "" {
		blob := make(stream.Blob, 1024)
		_, err = rand.Read(blob)
		if err != nil {
			log.Fatal("failed to make random blob: ", err)
		}

		err = c.SendBlob(blob)
		if err != nil {
			log.Error(err)
		}
		return
	}

	file, err := os.Open(path)
	checkErr(err)
	defer file.Close()
	s, err := stream.New(file)
	checkErr(err)

	sdBlob := &stream.SDBlob{}
	err = sdBlob.FromBlob(s[0])
	checkErr(err)

	for i, b := range s {
		if i == 0 {
			err = c.SendSDBlob(b)
		} else {
			err = c.SendBlob(b)
		}
		checkErr(err)
	}
}
