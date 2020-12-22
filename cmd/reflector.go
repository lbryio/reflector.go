package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/peer/http3"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/c2h5oh/datasize"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	tcpPeerPort                 int
	http3PeerPort               int
	receiverPort                int
	metricsPort                 int
	disableUploads              bool
	disableBlocklist            bool
	proxyAddress                string
	proxyPort                   string
	proxyProtocol               string
	useDB                       bool
	cloudFrontEndpoint          string
	reflectorCmdDiskCache       string
	bufferReflectorCmdDiskCache string
	reflectorCmdMemCache        int
)

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
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
	cmd.Flags().StringVar(&reflectorCmdDiskCache, "disk-cache", "",
		"enable disk cache, setting max size and path where to store blobs. format is 'sizeGB:CACHE_PATH'")
	cmd.Flags().StringVar(&bufferReflectorCmdDiskCache, "buffer-disk-cache", "",
		"enable buffer disk cache, setting max size and path where to store blobs. format is 'sizeGB:CACHE_PATH'")
	cmd.Flags().IntVar(&reflectorCmdMemCache, "mem-cache", 0, "enable in-memory cache with a max size of this many blobs")
	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector %s", meta.VersionString())

	// the blocklist logic requires the db backed store to be the outer-most store
	underlyingStore := setupStore()
	outerStore := wrapWithCache(underlyingStore)

	if !disableUploads {
		reflectorServer := reflector.NewServer(underlyingStore)
		reflectorServer.Timeout = 3 * time.Minute
		reflectorServer.EnableBlocklist = !disableBlocklist

		err := reflectorServer.Start(":" + strconv.Itoa(receiverPort))
		if err != nil {
			log.Fatal(err)
		}
		defer reflectorServer.Shutdown()
	}

	peerServer := peer.NewServer(outerStore)
	err := peerServer.Start(":" + strconv.Itoa(tcpPeerPort))
	if err != nil {
		log.Fatal(err)
	}
	defer peerServer.Shutdown()

	http3PeerServer := http3.NewServer(outerStore)
	err = http3PeerServer.Start(":" + strconv.Itoa(http3PeerPort))
	if err != nil {
		log.Fatal(err)
	}
	defer http3PeerServer.Shutdown()

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()
	defer metricsServer.Shutdown()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	// deferred shutdowns happen now
}

func setupStore() store.BlobStore {
	var s store.BlobStore

	if proxyAddress != "" {
		switch proxyProtocol {
		case "tcp":
			s = peer.NewStore(peer.StoreOpts{
				Address: proxyAddress + ":" + proxyPort,
				Timeout: 30 * time.Second,
			})
		case "http3":
			s = http3.NewStore(http3.StoreOpts{
				Address: proxyAddress + ":" + proxyPort,
				Timeout: 30 * time.Second,
			})
		default:
			log.Fatalf("protocol is not recognized: %s", proxyProtocol)
		}
	} else {
		var s3Store *store.S3Store
		if conf != "none" {
			s3Store = store.NewS3Store(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
		}
		if cloudFrontEndpoint != "" {
			cfs := store.NewCloudFrontROStore(cloudFrontEndpoint)
			if s3Store != nil {
				s = store.NewCloudFrontRWStore(cfs, s3Store)
			} else {
				s = cfs
			}
		} else if s3Store != nil {
			s = s3Store
		} else {
			log.Fatalf("this configuration does not include a valid upstream source")
		}
	}

	if useDB {
		db := new(db.SQL)
		db.TrackAccessTime = true
		err := db.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}

		s = store.NewDBBackedStore(s, db, false)
	}

	return s
}

func wrapWithCache(s store.BlobStore) store.BlobStore {
	wrapped := s

	diskCacheMaxSize, diskCachePath := diskCacheParams(reflectorCmdDiskCache)
	//we are tracking blobs in memory with a 1 byte long boolean, which means that for each 2MB (a blob) we need 1Byte
	// so if the underlying cache holds 10MB, 10MB/2MB=5Bytes which is also the exact count of objects to restore on startup
	realCacheSize := float64(diskCacheMaxSize) / float64(stream.MaxBlobSize)
	if diskCacheMaxSize > 0 {
		err := os.MkdirAll(diskCachePath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		wrapped = store.NewCachingStore(
			"reflector",
			wrapped,
			store.NewLFUDAStore("hdd", store.NewDiskStore(diskCachePath, 2), realCacheSize),
		)
	}

	diskCacheMaxSize, diskCachePath = diskCacheParams(bufferReflectorCmdDiskCache)
	realCacheSize = float64(diskCacheMaxSize) / float64(stream.MaxBlobSize)
	if diskCacheMaxSize > 0 {
		err := os.MkdirAll(diskCachePath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		wrapped = store.NewCachingStore(
			"reflector",
			wrapped,
			store.NewLFUDAStore("nvme", store.NewDiskStore(diskCachePath, 2), realCacheSize),
		)
	}

	if reflectorCmdMemCache > 0 {
		wrapped = store.NewCachingStore(
			"reflector",
			wrapped,
			store.NewLRUStore("mem", store.NewMemStore(), reflectorCmdMemCache),
		)
	}

	return wrapped
}

func diskCacheParams(diskParams string) (int, string) {
	if diskParams == "" {
		return 0, ""
	}

	parts := strings.Split(diskParams, ":")
	if len(parts) != 2 {
		log.Fatalf("--disk-cache must be a number, followed by ':', followed by a string")
	}

	diskCacheSize := parts[0]
	path := parts[1]
	if len(path) == 0 || path[0] != '/' {
		log.Fatalf("--disk-cache path must start with '/'")
	}

	var maxSize datasize.ByteSize
	err := maxSize.UnmarshalText([]byte(diskCacheSize))
	if err != nil {
		log.Fatal(err)
	}
	if maxSize <= 0 {
		log.Fatal("--disk-cache size must be more than 0")
	}
	return int(maxSize), path
}
