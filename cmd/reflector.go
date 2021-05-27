package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/peer/http3"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/server/http"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/c2h5oh/datasize"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	tcpPeerPort                 int
	http3PeerPort               int
	httpPort                    int
	receiverPort                int
	metricsPort                 int
	disableUploads              bool
	disableBlocklist            bool
	proxyAddress                string
	proxyPort                   string
	proxyProtocol               string
	useDB                       bool
	cloudFrontEndpoint          string
	WasabiEndpoint              string
	reflectorCmdDiskCache       string
	bufferReflectorCmdDiskCache string
	reflectorCmdMemCache        int
	requestQueueSize            int
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
	cmd.Flags().StringVar(&WasabiEndpoint, "wasabi-endpoint", "", "Wasabi edge endpoint for standard HTTP retrieval")
	cmd.Flags().IntVar(&tcpPeerPort, "tcp-peer-port", 5567, "The port reflector will distribute content from")
	cmd.Flags().IntVar(&http3PeerPort, "http3-peer-port", 5568, "The port reflector will distribute content from over HTTP3 protocol")
	cmd.Flags().IntVar(&httpPort, "http-port", 5569, "The port reflector will distribute content from over HTTP protocol")
	cmd.Flags().IntVar(&receiverPort, "receiver-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for metrics")
	cmd.Flags().IntVar(&requestQueueSize, "request-queue-size", 200, "How many concurrent requests should be submitted to upstream")
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
	cleanerStopper := stop.New()

	// the blocklist logic requires the db backed store to be the outer-most store
	underlyingStore := setupStore()
	outerStore := wrapWithCache(underlyingStore, cleanerStopper)

	if !disableUploads {
		reflectorServer := reflector.NewServer(underlyingStore, outerStore)
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

	http3PeerServer := http3.NewServer(outerStore, requestQueueSize)
	err = http3PeerServer.Start(":" + strconv.Itoa(http3PeerPort))
	if err != nil {
		log.Fatal(err)
	}
	defer http3PeerServer.Shutdown()

	httpServer := http.NewServer(outerStore, requestQueueSize)
	err = httpServer.Start(":" + strconv.Itoa(httpPort))
	if err != nil {
		log.Fatal(err)
	}
	defer httpServer.Shutdown()

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()
	defer metricsServer.Shutdown()
	defer outerStore.Shutdown()
	defer underlyingStore.Shutdown()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	// deferred shutdowns happen now
	cleanerStopper.StopAndWait()
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
		case "http":
			s = store.NewHttpStore(proxyAddress + ":" + proxyPort)
		default:
			log.Fatalf("protocol is not recognized: %s", proxyProtocol)
		}
	} else {
		var s3Store *store.S3Store
		if conf != "none" {
			s3Store = store.NewS3Store(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
		}
		if cloudFrontEndpoint != "" && WasabiEndpoint != "" {
			ittt := store.NewITTTStore(store.NewCloudFrontROStore(WasabiEndpoint), store.NewCloudFrontROStore(cloudFrontEndpoint))
			if s3Store != nil {
				s = store.NewCloudFrontRWStore(ittt, s3Store)
			} else {
				s = ittt
			}
		} else if s3Store != nil {
			s = s3Store
		} else {
			log.Fatalf("this configuration does not include a valid upstream source")
		}
	}

	if useDB {
		dbInst := &db.SQL{
			TrackAccess: db.TrackAccessStreams,
			LogQueries:  log.GetLevel() == log.DebugLevel,
		}
		err := dbInst.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}

		s = store.NewDBBackedStore(s, dbInst, false)
	}

	return s
}

func wrapWithCache(s store.BlobStore, cleanerStopper *stop.Group) store.BlobStore {
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

		localDb := &db.SQL{
			SoftDelete:  true,
			TrackAccess: db.TrackAccessBlobs,
			LogQueries:  log.GetLevel() == log.DebugLevel,
		}
		err = localDb.Connect("reflector:reflector@tcp(localhost:3306)/reflector")
		if err != nil {
			log.Fatal(err)
		}
		dbBackedDiskStore := store.NewDBBackedStore(store.NewDiskStore(diskCachePath, 2), localDb, true)
		wrapped = store.NewCachingStore(
			"reflector",
			wrapped,
			dbBackedDiskStore,
		)

		go cleanOldestBlobs(int(realCacheSize), localDb, dbBackedDiskStore, cleanerStopper)
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

func cleanOldestBlobs(maxItems int, db *db.SQL, store store.BlobStore, stopper *stop.Group) {
	// this is so that it runs on startup without having to wait for 10 minutes
	err := doClean(maxItems, db, store, stopper)
	if err != nil {
		log.Error(errors.FullTrace(err))
	}
	const cleanupInterval = 10 * time.Minute
	for {
		select {
		case <-stopper.Ch():
			log.Infoln("stopping self cleanup")
			return
		case <-time.After(cleanupInterval):
			err := doClean(maxItems, db, store, stopper)
			if err != nil {
				log.Error(errors.FullTrace(err))
			}
		}
	}
}

func doClean(maxItems int, db *db.SQL, store store.BlobStore, stopper *stop.Group) error {
	blobsCount, err := db.Count()
	if err != nil {
		return err
	}

	if blobsCount >= maxItems {
		itemsToDelete := blobsCount / 10
		blobs, err := db.LeastRecentlyAccessedHashes(itemsToDelete)
		if err != nil {
			return err
		}
		blobsChan := make(chan string, len(blobs))
		wg := &stop.Group{}
		go func() {
			for _, hash := range blobs {
				select {
				case <-stopper.Ch():
					return
				default:
				}
				blobsChan <- hash
			}
			close(blobsChan)
		}()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for h := range blobsChan {
					select {
					case <-stopper.Ch():
						return
					default:
					}
					err = store.Delete(h)
					if err != nil {
						log.Errorf("error pruning %s: %s", h, errors.FullTrace(err))
						continue
					}
				}
			}()
		}
		wg.Wait()
	}
	return nil
}
