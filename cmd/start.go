package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/prism"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	startPeerPort        int
	startReflectorPort   int
	startDhtPort         int
	startDhtSeedPort     int
	startClusterPort     int
	startClusterSeedPort int
)

func init() {
	var cmd = &cobra.Command{
		Use:   "start [cluster-address]",
		Short: "Runs prism application with cluster, dht, peer server, and reflector server.",
		Run:   startCmd,
		Args:  cobra.RangeArgs(0, 1),
	}
	cmd.PersistentFlags().IntVar(&startDhtPort, "dht-port", 4570, "Port to start DHT on")
	cmd.PersistentFlags().IntVar(&startDhtSeedPort, "dht-seed-port", 4567, "Port to connect to DHT bootstrap node on")
	cmd.PersistentFlags().IntVar(&startClusterPort, "cluster-port", 5678, "Port to start DHT on")
	cmd.PersistentFlags().IntVar(&startClusterSeedPort, "cluster-seed-port", 0, "Port to start DHT on")
	cmd.PersistentFlags().IntVar(&startPeerPort, "peer-port", peer.DefaultPort, "Port to start peer protocol on")
	cmd.PersistentFlags().IntVar(&startReflectorPort, "reflector-port", reflector.DefaultPort, "Port to start reflector protocol on")

	rootCmd.AddCommand(cmd)
}

func startCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)
	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	comboStore := store.NewDBBackedS3Store(s3, db)

	// TODO: args we need:
	// clusterAddr - to connect to cluster (or start new cluster if empty)
	// minNodes - minimum number of nodes before announcing starts. otherwise first node will try to announce all the blobs in the db
	//     or maybe we should do maxHashesPerNode?
	//     in either case, this should not kill the cluster, but should only limit announces (and notify when some hashes are being left unannounced)

	//clusterAddr := ""
	//if len(args) > 0 {
	//	clusterAddr = args[0]
	//}

	conf := prism.DefaultConf()
	conf.DB = db
	conf.Blobs = comboStore
	conf.DhtAddress = "127.0.0.1:" + strconv.Itoa(startDhtPort)
	conf.DhtSeedNodes = []string{"127.0.0.1:" + strconv.Itoa(startDhtSeedPort)}
	conf.ClusterPort = startClusterPort
	if startClusterSeedPort > 0 {
		conf.ClusterSeedAddr = "127.0.0.1:" + strconv.Itoa(startClusterSeedPort)
	}

	p := prism.New(conf)
	err = p.Start()
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	p.Shutdown()
}
