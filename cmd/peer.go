package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/server/peer"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var peerNoDB bool

func init() {
	var cmd = &cobra.Command{
		Use:   "peer",
		Short: "Run peer server",
		Run:   peerCmd,
	}
	cmd.Flags().BoolVar(&peerNoDB, "nodb", false, "Don't connect to a db and don't use a db-backed blob store")
	rootCmd.AddCommand(cmd)
}

func peerCmd(cmd *cobra.Command, args []string) {
	var err error

	s3 := store.NewS3Store(store.S3Params{
		Name:      "peer",
		AwsID:     globalConfig.AwsID,
		AwsSecret: globalConfig.AwsSecret,
		Region:    globalConfig.BucketRegion,
		Bucket:    globalConfig.BucketName,
		Endpoint:  globalConfig.S3Endpoint,
	})
	peerServer := peer.NewServer(s3, fmt.Sprintf(":%d", peer.DefaultPort))

	if !peerNoDB {
		db := &db.SQL{
			LogQueries: log.GetLevel() == log.DebugLevel,
		}
		err = db.Connect(globalConfig.DBConn)
		checkErr(err)

		combo := store.NewDBBackedStore(store.DBBackedParams{
			Name:         "peer",
			Store:        s3,
			DB:           db,
			DeleteOnMiss: false,
			MaxSize:      nil,
		})
		peerServer = peer.NewServer(combo, fmt.Sprintf(":%d", peer.DefaultPort))
	}

	err = peerServer.Start()
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	peerServer.Shutdown()
}
