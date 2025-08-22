package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/server/peer"
	"github.com/lbryio/reflector.go/store"

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
	log.Printf("reflector %s", meta.VersionString())

	memStore := store.NewMemStore(store.MemParams{Name: "test"})

	reflectorServer := reflector.NewIngestionServer(memStore)
	reflectorServer.Timeout = 3 * time.Minute

	err := reflectorServer.Start(":" + strconv.Itoa(reflector.DefaultPort))
	if err != nil {
		log.Fatal(err)
	}

	peerServer := peer.NewServer(memStore, fmt.Sprintf(":%d", reflector.DefaultPort+1))
	err = peerServer.Start()
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	peerServer.Shutdown()
	reflectorServer.Shutdown()

	fmt.Println("Blobs in store")
	for hash, blob := range memStore.Debug() {
		fmt.Printf("%s: %d bytes\n", hash, len(blob))
	}
}
