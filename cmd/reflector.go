package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/peer/quic"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var reflectorCmdCacheDir string
var peerPort int
var quicPeerPort int
var reflectorPort int
var metricsPort int

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
	cmd.Flags().StringVar(&reflectorCmdCacheDir, "cache", "", "Enable disk cache for blobs. Store them in this directory")
	cmd.Flags().IntVar(&peerPort, "peer-port", 5567, "The port reflector will distribute content from")
	cmd.Flags().IntVar(&quicPeerPort, "quic-peer-port", 5568, "The port reflector will distribute content from over QUIC protocol")
	cmd.Flags().IntVar(&reflectorPort, "reflector-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for metrics")
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

		err = reflectorServer.Start(":" + strconv.Itoa(reflectorPort))
		if err != nil {
			log.Fatal(err)
		}
	}

	if reflectorCmdCacheDir != "" {
		err = os.MkdirAll(reflectorCmdCacheDir, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		blobStore = store.NewCachingBlobStore(blobStore, store.NewDiskBlobStore(reflectorCmdCacheDir, 2))
	}

	peerServer := peer.NewServer(blobStore)
	err = peerServer.Start(":"+ strconv.Itoa(peerPort))
	if err != nil {
		log.Fatal(err)
	}

	quicPeerServer := quic.NewServer(blobStore)
	err = quicPeerServer.Start(":"+ strconv.Itoa(quicPeerPort))
	if err != nil {
		log.Fatal(err)
	}

	metricsServer := metrics.NewServer(":" + strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	metricsServer.Shutdown()
	peerServer.Shutdown()
	quicPeerServer.Shutdown()
	if reflectorServer != nil {
		reflectorServer.Shutdown()
	}
}
