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
	"github.com/lbryio/reflector.go/peer/http3"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var reflectorCmdCacheDir string
var tcpPeerPort int
var http3PeerPort int
var receiverPort int
var metricsPort int
var disableUploads bool
var disableBlocklist bool
var proxyAddress string
var proxyPort string
var proxyProtocol string
var useDB bool
var cloudFrontEndpoint string

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
	cmd.Flags().StringVar(&reflectorCmdCacheDir, "cache", "", "if specified, the path where blobs should be cached (disabled when left empty)")
	cmd.Flags().StringVar(&proxyAddress, "proxy-address", "", "address of another reflector server where blobs are fetched from")
	cmd.Flags().StringVar(&proxyPort, "proxy-port", "5567", "port of another reflector server where blobs are fetched from")
	cmd.Flags().StringVar(&proxyProtocol, "proxy-protocol", "http3", "protocol used to fetch blobs from another reflector server (tcp/http3)")
	cmd.Flags().StringVar(&cloudFrontEndpoint, "cloudfront-endpoint", "", "CloudFront edge endpoint for standard HTTP retrieval")
	cmd.Flags().IntVar(&tcpPeerPort, "tcp-peer-port", 5567, "The port reflector will distribute content from")
	cmd.Flags().IntVar(&http3PeerPort, "http3-peer-port", 5568, "The port reflector will distribute content from over HTTP3 protocol")
	cmd.Flags().IntVar(&receiverPort, "receiver-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for metrics")
	cmd.Flags().BoolVar(&disableUploads, "disable-uploads", false, "Disable uploads to this reflector server")
	cmd.Flags().BoolVar(&disableBlocklist, "disable-blocklist", false, "Disable blocklist watching/updating")
	cmd.Flags().BoolVar(&useDB, "use-db", true, "whether to connect to the reflector db or not")
	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector %s", meta.VersionString())

	var blobStore store.BlobStore
	if proxyAddress != "" {
		switch proxyProtocol {
		case "tcp":
			blobStore = peer.NewStore(peer.StoreOpts{
				Address: proxyAddress + ":" + proxyPort,
				Timeout: 30 * time.Second,
			})
		case "http3":
			blobStore = http3.NewStore(http3.StoreOpts{
				Address: proxyAddress + ":" + proxyPort,
				Timeout: 30 * time.Second,
			})
		default:
			log.Fatalf("specified protocol is not recognized: %s", proxyProtocol)
		}
	} else {
		s3Store := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
		if cloudFrontEndpoint != "" {
			blobStore = store.NewCloudFrontBlobStore(cloudFrontEndpoint, s3Store)
		} else {
			blobStore = s3Store
		}
	}

	var err error
	var reflectorServer *reflector.Server

	if useDB {
		db := new(db.SQL)
		db.TrackAccessTime = true
		err = db.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}

		blobStore = store.NewDBBackedStore(blobStore, db)

		//this shouldn't go here but the blocklist logic requires the db backed store to be the outer-most store for it to work....
		//having this here prevents uploaded blobs from being stored in the disk cache
		if !disableUploads {
			reflectorServer = reflector.NewServer(blobStore)
			reflectorServer.Timeout = 3 * time.Minute
			reflectorServer.EnableBlocklist = !disableBlocklist

			err = reflectorServer.Start(":" + strconv.Itoa(receiverPort))
			if err != nil {
				log.Fatal(err)
			}
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
	err = peerServer.Start(":" + strconv.Itoa(tcpPeerPort))
	if err != nil {
		log.Fatal(err)
	}

	http3PeerServer := http3.NewServer(blobStore)
	err = http3PeerServer.Start(":" + strconv.Itoa(http3PeerPort))
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
	http3PeerServer.Shutdown()
	if reflectorServer != nil {
		reflectorServer.Shutdown()
	}
}
