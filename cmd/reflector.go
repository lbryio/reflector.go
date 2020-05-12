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
var disableUploads bool
var reflectorServerAddress string
var reflectorServerPort string
var reflectorServerProtocol string
var useDB bool

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
	cmd.Flags().StringVar(&reflectorCmdCacheDir, "cache", "", "Enable disk cache for blobs. Store them in this directory")
	cmd.Flags().StringVar(&reflectorServerAddress, "reflector-server-address", "", "address of another reflector server where blobs are fetched from")
	cmd.Flags().StringVar(&reflectorServerPort, "reflector-server-port", "5567", "port of another reflector server where blobs are fetched from")
	cmd.Flags().StringVar(&reflectorServerProtocol, "reflector-server-protocol", "tcp", "protocol used to fetch blobs from another reflector server (tcp/udp)")
	cmd.Flags().IntVar(&peerPort, "peer-port", 5567, "The port reflector will distribute content from")
	cmd.Flags().IntVar(&quicPeerPort, "quic-peer-port", 5568, "The port reflector will distribute content from over QUIC protocol")
	cmd.Flags().IntVar(&reflectorPort, "reflector-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for metrics")
	cmd.Flags().BoolVar(&disableUploads, "disable-uploads", false, "Disable uploads to this reflector server")
	cmd.Flags().BoolVar(&useDB, "use-db", true, "whether to connect to the reflector db or not")
	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector version %s, built %s", meta.Version, meta.BuildTime.Format(time.RFC3339))

	var blobStore store.BlobStore
	if reflectorServerAddress != "" {
		switch reflectorServerProtocol {
		case "tcp":
			blobStore = peer.NewStore(peer.StoreOpts{
				Address: reflectorServerAddress + ":" + reflectorServerPort,
				Timeout: 10 * time.Second,
			})
		case "udp":
			blobStore = quic.NewStore(quic.StoreOpts{
				Address: reflectorServerAddress + ":" + reflectorServerPort,
				Timeout: 10 * time.Second,
			})
		}
	} else {
		blobStore = store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	}

	var err error
	var reflectorServer *reflector.Server

	if useDB {
		db := new(db.SQL)
		err = db.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}

		blobStore = store.NewDBBackedStore(blobStore, db)

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
	err = peerServer.Start(":" + strconv.Itoa(peerPort))
	if err != nil {
		log.Fatal(err)
	}

	quicPeerServer := quic.NewServer(blobStore)
	err = quicPeerServer.Start(":" + strconv.Itoa(quicPeerPort))
	if err != nil {
		log.Fatal(err)
	}

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
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
