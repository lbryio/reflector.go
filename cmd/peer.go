package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "peer",
		Short: "Run peer server",
		Run:   peerCmd,
	}
	rootCmd.AddCommand(cmd)
}

func peerCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	combo := store.NewDBBackedS3Store(s3, db)
	peerServer := peer.NewServer(combo)
	if err := peerServer.Start("localhost:" + strconv.Itoa(peer.DefaultPort)); err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	peerServer.Shutdown()
}
