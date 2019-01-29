package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "test",
		Short: "Test things",
		Run:   testCmd,
	}
	rootCmd.AddCommand(cmd)
}

func testCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector version %s", meta.Version)

	memStore := &store.MemoryBlobStore{}

	reflectorServer := reflector.NewServer(memStore)
	reflectorServer.Timeout = 3 * time.Minute

	err := reflectorServer.Start(":" + strconv.Itoa(reflector.DefaultPort))
	if err != nil {
		log.Fatal(err)
	}

	peerServer := peer.NewServer(memStore)
	err = peerServer.Start(":5567")
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	peerServer.Shutdown()
	reflectorServer.Shutdown()
	spew.Dump(memStore)
}
