package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
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
	log.Printf("reflector version %s, built %s", meta.Version, meta.BuildTime.Format(time.RFC3339))

	// flip this flag to false when doing db maintenance. uploads will not work (as reflector server wont be running)
	// but downloads will still work straight from s3
	useDB := true

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)

	var err error

	var blobStore store.BlobStore = s3
	var reflectorServer *reflector.Server

	if useDB {
		db := new(db.SQL)
		err = db.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}

		blobStore = store.NewDBBackedStore(s3, db)

		reflectorServer = reflector.NewServer(blobStore)
		reflectorServer.Timeout = 3 * time.Minute
		reflectorServer.EnableBlocklist = true

		err = reflectorServer.Start(":" + strconv.Itoa(reflector.DefaultPort))
		if err != nil {
			log.Fatal(err)
		}
	}

	peerServer := peer.NewServer(blobStore)
	err = peerServer.Start(":5567")
	if err != nil {
		log.Fatal(err)
	}

	metricsServer := metrics.NewServer(":2112", "/metrics")
	metricsServer.Start()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	metricsServer.Shutdown()
	peerServer.Shutdown()
	if reflectorServer != nil {
		reflectorServer.Shutdown()
	}
}
