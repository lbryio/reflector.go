package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector version %s", meta.Version)
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	if err != nil {
		log.Fatal(err)
	}

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	combo := store.NewDBBackedS3Store(s3, db)

	reflectorServer := reflector.NewServer(combo)
	reflectorServer.Timeout = 3 * time.Minute
	if globalConfig.SlackHookURL != "" {
		reflectorServer.StatLogger = log.StandardLogger()
		reflectorServer.StatReportFrequency = 1 * time.Hour
	}
	reflectorServer.EnableBlocklist = true

	err = reflectorServer.Start(":" + strconv.Itoa(reflector.DefaultPort))
	if err != nil {
		log.Fatal(err)
	}

	peerServer := peer.NewServer(combo)
	if globalConfig.SlackHookURL != "" {
		peerServer.StatLogger = log.StandardLogger()
		peerServer.StatReportFrequency = 1 * time.Hour
	}
	err = peerServer.Start(":5567")
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	peerServer.Shutdown()
	reflectorServer.Shutdown()
}
