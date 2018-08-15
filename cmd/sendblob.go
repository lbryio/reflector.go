package cmd

import (
	"crypto/rand"

	"github.com/lbryio/reflector.go/reflector"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "sendblob ADDRESS:PORT",
		Short: "Send a random blob to a reflector server",
		Args:  cobra.ExactArgs(1),
		Run:   sendBlobCmd,
	}
	rootCmd.AddCommand(cmd)
}

func sendBlobCmd(cmd *cobra.Command, args []string) {
	addr := args[0]

	c := reflector.Client{}
	err := c.Connect(addr)
	if err != nil {
		log.Fatal("error connecting client to server", err)
	}

	blob := make([]byte, 1024)
	_, err = rand.Read(blob)
	if err != nil {
		log.Fatal("failed to make random blob", err)
	}

	err = c.SendBlob(blob)
	if err != nil {
		log.Error(err)
	}
}
