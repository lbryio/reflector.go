package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/server/http"
	"github.com/lbryio/reflector.go/server/http3"
	"github.com/lbryio/reflector.go/server/peer"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/c2h5oh/datasize"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	//port configuration
	tcpPeerPort   int
	http3PeerPort int
	httpPeerPort  int
	receiverPort  int
	metricsPort   int

	//flags configuration
	disableUploads   bool
	disableBlocklist bool
	useDB            bool

	//upstream configuration
	upstreamReflector string
	upstreamProtocol  string
	upstreamEdgeToken string

	//downstream configuration
	requestQueueSize int

	//upstream edge configuration (to "cold" storage)
	originEndpoint         string
	originEndpointFallback string

	//cache configuration
	diskCache          string
	secondaryDiskCache string
	memCache           int
)
var cacheManagers = []string{"localdb", "lfu", "arc", "lru", "simple"}

var cacheMangerToGcache = map[string]store.EvictionStrategy{
	"lfu":    store.LFU,
	"arc":    store.ARC,
	"lru":    store.LRU,
	"simple": store.SIMPLE,
}

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}

	cmd.Flags().IntVar(&tcpPeerPort, "tcp-peer-port", 5567, "The port reflector will distribute content from for the TCP (LBRY) protocol")
	cmd.Flags().IntVar(&http3PeerPort, "http3-peer-port", 5568, "The port reflector will distribute content from over HTTP3 protocol")
	cmd.Flags().IntVar(&httpPeerPort, "http-peer-port", 5569, "The port reflector will distribute content from over HTTP protocol")
	cmd.Flags().IntVar(&receiverPort, "receiver-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for prometheus metrics")

	cmd.Flags().BoolVar(&disableUploads, "disable-uploads", false, "Disable uploads to this reflector server")
	cmd.Flags().BoolVar(&disableBlocklist, "disable-blocklist", false, "Disable blocklist watching/updating")
	cmd.Flags().BoolVar(&useDB, "use-db", true, "Whether to connect to the reflector db or not")

	cmd.Flags().StringVar(&upstreamReflector, "upstream-reflector", "", "host:port of a reflector server where blobs are fetched from")
	cmd.Flags().StringVar(&upstreamProtocol, "upstream-protocol", "http", "protocol used to fetch blobs from another upstream reflector server (tcp/http3/http)")
	cmd.Flags().StringVar(&upstreamEdgeToken, "upstream-edge-token", "", "token used to retrieve/authenticate protected content")

	cmd.Flags().IntVar(&requestQueueSize, "request-queue-size", 200, "How many concurrent requests from downstream should be handled at once (the rest will wait)")

	cmd.Flags().StringVar(&originEndpoint, "origin-endpoint", "", "HTTP edge endpoint for standard HTTP retrieval")
	cmd.Flags().StringVar(&originEndpointFallback, "origin-endpoint-fallback", "", "HTTP edge endpoint for standard HTTP retrieval if first origin fails")

	cmd.Flags().StringVar(&diskCache, "disk-cache", "100GB:/tmp/downloaded_blobs:localdb", "Where to cache blobs on the file system. format is 'sizeGB:CACHE_PATH:cachemanager' (cachemanagers: localdb/lfu/arc/lru)")
	cmd.Flags().StringVar(&secondaryDiskCache, "optional-disk-cache", "", "Optional secondary file system cache for blobs. format is 'sizeGB:CACHE_PATH:cachemanager' (cachemanagers: localdb/lfu/arc/lru) (this would get hit before the one specified in disk-cache)")
	cmd.Flags().IntVar(&memCache, "mem-cache", 0, "enable in-memory cache with a max size of this many blobs")

	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector %s", meta.VersionString())

	// the blocklist logic requires the db backed store to be the outer-most store
	underlyingStore := initStores()
	underlyingStoreWithCaches := initCaches(underlyingStore)

	if !disableUploads {
		reflectorServer := reflector.NewServer(underlyingStore, underlyingStoreWithCaches)
		reflectorServer.Timeout = 3 * time.Minute
		reflectorServer.EnableBlocklist = !disableBlocklist

		err := reflectorServer.Start(":" + strconv.Itoa(receiverPort))
		if err != nil {
			log.Fatal(err)
		}
		defer reflectorServer.Shutdown()
	}

	peerServer := peer.NewServer(underlyingStoreWithCaches, fmt.Sprintf(":%d", tcpPeerPort))
	err := peerServer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer peerServer.Shutdown()

	http3PeerServer := http3.NewServer(underlyingStoreWithCaches, requestQueueSize, fmt.Sprintf(":%d", http3PeerPort))
	err = http3PeerServer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer http3PeerServer.Shutdown()

	httpServer := http.NewServer(store.WithSingleFlight("sf-http", underlyingStoreWithCaches), requestQueueSize, upstreamEdgeToken, fmt.Sprintf(":%d", httpPeerPort))
	err = httpServer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer httpServer.Shutdown()

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()
	defer metricsServer.Shutdown()
	defer underlyingStoreWithCaches.Shutdown()
	defer underlyingStore.Shutdown() //do we actually need this? Oo

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
}

func initUpstreamStore() store.BlobStore {
	var s store.BlobStore
	if upstreamReflector == "" {
		return nil
	}
	switch upstreamProtocol {
	case "tcp":
		s = store.NewPeerStore(store.PeerParams{
			Name:    "reflector",
			Address: upstreamReflector,
			Timeout: 30 * time.Second,
		})
	case "http3":
		s = store.NewHttp3Store(store.Http3Params{
			Name:    "reflector",
			Address: upstreamReflector,
			Timeout: 30 * time.Second,
		})
	case "http":
		s = store.NewUpstreamStore(store.UpstreamParams{
			Name:      "reflector",
			Upstream:  upstreamReflector,
			EdgeToken: upstreamEdgeToken,
		})
	default:
		log.Fatalf("protocol is not recognized: %s", upstreamProtocol)
	}
	return s
}

func initEdgeStore() store.BlobStore {
	var s3Store *store.S3Store
	var s store.BlobStore

	if conf != "none" {
		s3Store = store.NewS3Store(store.S3Params{
			Name:      "reflector",
			AwsID:     globalConfig.AwsID,
			AwsSecret: globalConfig.AwsSecret,
			Region:    globalConfig.BucketRegion,
			Bucket:    globalConfig.BucketName,
			Endpoint:  globalConfig.S3Endpoint,
		})
	}
	if originEndpointFallback != "" && originEndpoint != "" {
		ittt := store.NewITTTStore(store.ITTTParams{
			Name: "reflector",
			This: store.NewHttpStore(store.HttpParams{
				Name:         "owns3",
				Endpoint:     originEndpoint,
				ShardingSize: 0,
			}),
			That: store.NewHttpStore(store.HttpParams{
				Name:         "wasabi",
				Endpoint:     originEndpointFallback,
				ShardingSize: 0,
			}),
		})
		if s3Store != nil {
			s = store.NewProxiedS3Store(store.ProxiedS3Params{
				Name:   "reflector",
				Reader: ittt,
				Writer: s3Store,
			})
		} else {
			s = ittt
		}
	} else if s3Store != nil {
		s = s3Store
	} else {
		log.Fatalf("this configuration does not include a valid upstream source")
	}
	return s
}

func initDBStore(s store.BlobStore) store.BlobStore {
	if useDB {
		dbInst := &db.SQL{
			TrackingLevel: db.TrackAccessStreams,
			LogQueries:    log.GetLevel() == log.DebugLevel,
		}
		err := dbInst.Connect(globalConfig.DBConn)
		if err != nil {
			log.Fatal(err)
		}
		s = store.NewDBBackedStore(store.DBBackedParams{
			Name:         "global",
			Store:        s,
			DB:           dbInst,
			DeleteOnMiss: false,
			MaxSize:      nil,
		})
	}
	return s
}

func initStores() store.BlobStore {
	s := initUpstreamStore()
	if s == nil {
		s = initEdgeStore()
	}
	s = initDBStore(s)
	return s
}

// initCaches returns a store wrapped with caches and a stop group to execute a clean shutdown
func initCaches(s store.BlobStore) store.BlobStore {
	diskStore := initDiskStore(s, diskCache)
	finalStore := initDiskStore(diskStore, secondaryDiskCache)
	if memCache > 0 {
		finalStore = store.NewCachingStore(store.CachingParams{
			Name:   "reflector",
			Origin: finalStore,
			Cache: store.NewGcacheStore(store.GcacheParams{
				Name:     "volatile-cache",
				Store:    store.NewMemStore(store.MemParams{Name: "volatile-cache"}),
				MaxSize:  memCache,
				Strategy: store.LRU,
			}),
		})
	}
	return finalStore
}

func initDiskStore(upstreamStore store.BlobStore, diskParams string) store.BlobStore {
	diskCacheMaxSize, diskCachePath, cacheManager := diskCacheParams(diskParams)
	//we are tracking blobs in memory with a 1 byte long boolean, which means that for each 2MB (a blob) we need 1Byte
	// so if the underlying cache holds 10MB, 10MB/2MB=5Bytes which is also the exact count of objects to restore on startup
	realCacheSize := int(float64(diskCacheMaxSize) / float64(stream.MaxBlobSize))
	if diskCacheMaxSize == 0 {
		return upstreamStore
	}
	err := os.MkdirAll(diskCachePath, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	diskStore := store.NewDiskStore(store.DiskParams{
		Name:         "big-drive",
		MountPoint:   diskCachePath,
		ShardingSize: 2,
	})
	var unwrappedStore store.BlobStore

	if cacheManager == "localdb" {
		localDb := &db.SQL{
			SoftDelete:    true,
			TrackingLevel: db.TrackAccessBlobs,
			LogQueries:    log.GetLevel() == log.DebugLevel,
		}
		err = localDb.Connect("reflector:reflector@tcp(localhost:3306)/reflector")
		if err != nil {
			log.Fatal(err)
		}
		unwrappedStore = store.NewDBBackedStore(store.DBBackedParams{
			Name:         "local",
			Store:        diskStore,
			DB:           localDb,
			DeleteOnMiss: true,
			MaxSize:      &realCacheSize,
		})
	} else {
		unwrappedStore = store.NewGcacheStore(store.GcacheParams{
			Name:     "flash",
			Store:    store.NewDiskStore(store.DiskParams{Name: "flash", MountPoint: diskCachePath, ShardingSize: 2}),
			MaxSize:  realCacheSize,
			Strategy: cacheMangerToGcache[cacheManager],
		})
	}

	wrapped := store.NewCachingStore(store.CachingParams{
		Name:   "reflector",
		Origin: upstreamStore,
		Cache:  unwrappedStore,
	})
	return wrapped
}

func diskCacheParams(diskParams string) (int, string, string) {
	if diskParams == "" {
		return 0, "", ""
	}

	parts := strings.Split(diskParams, ":")
	if len(parts) != 3 {
		log.Fatalf("%s does is formatted incorrectly. Expected format: 'sizeGB:CACHE_PATH:cachemanager' for example: '100GB:/tmp/downloaded_blobs:localdb'", diskParams)
	}

	diskCacheSize := parts[0]
	path := parts[1]
	cacheManager := parts[2]

	if len(path) == 0 || path[0] != '/' {
		log.Fatalf("disk cache paths must start with '/'")
	}

	if !util.InSlice(cacheManager, cacheManagers) {
		log.Fatalf("specified cache manager '%s' is not supported. Use one of the following: %v", cacheManager, cacheManagers)
	}

	var maxSize datasize.ByteSize
	err := maxSize.UnmarshalText([]byte(diskCacheSize))
	if err != nil {
		log.Fatal(err)
	}
	if maxSize <= 0 {
		log.Fatal("disk cache size must be more than 0")
	}
	return int(maxSize), path, cacheManager
}
