package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/lbryio/lbry.go/dht"
	"github.com/lbryio/lbry.go/dht/bits"
	"github.com/lbryio/reflector.go/cluster"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/prism"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	startNewCluster = "new"
)

var (
	startClusterPort   int
	startPeerPort      int
	startReflectorPort int
	startDhtPort       int
	startDhtSeeds      []string
	startHashRange     string
)

func init() {
	var cmd = &cobra.Command{
		Use:   `start [cluster-address|"new"]`,
		Short: "Runs full prism application with cluster, dht, peer server, and reflector server.",
		Run:   startCmd,
		Args:  cobra.ExactArgs(1),
	}
	cmd.PersistentFlags().IntVar(&startClusterPort, "cluster-port", cluster.DefaultPort, "Port that cluster listens on")
	cmd.PersistentFlags().IntVar(&startPeerPort, "peer-port", peer.DefaultPort, "Port to start peer protocol on")
	cmd.PersistentFlags().IntVar(&startReflectorPort, "reflector-port", reflector.DefaultPort, "Port to start reflector protocol on")
	cmd.PersistentFlags().IntVar(&startDhtPort, "dht-port", dht.DefaultPort, "Port that dht will listen on")
	cmd.PersistentFlags().StringSliceVar(&startDhtSeeds, "dht-seeds", []string{}, "Comma-separated list of dht seed nodes (addr:port,addr:port,...)")

	cmd.PersistentFlags().StringVar(&startHashRange, "hash-range", "", "Limit on range of hashes to announce (start-end)")

	rootCmd.AddCommand(cmd)
}

func startCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)
	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	comboStore := store.NewDBBackedS3Store(s3, db)

	conf := prism.DefaultConf()

	// TODO: args we need:
	// minNodes - minimum number of nodes before announcing starts. otherwise first node will try to announce all the blobs in the db
	//     or maybe we should do maxHashesPerNode?
	//     in either case, this should not kill the cluster, but should only limit announces (and notify when some hashes are being left unannounced)

	if args[0] != startNewCluster {
		conf.ClusterSeedAddr = args[0]
	}

	conf.DB = db
	conf.Blobs = comboStore
	conf.DhtAddress = "0.0.0.0:" + strconv.Itoa(startDhtPort)
	conf.DhtSeedNodes = startDhtSeeds
	conf.ClusterPort = startClusterPort
	conf.PeerPort = startPeerPort
	conf.ReflectorPort = startReflectorPort

	if startHashRange != "" {
		hashRange := strings.Split(startHashRange, "-")
		if len(hashRange) != 2 {
			log.Fatal("invalid hash range")
		}
		r := bits.Range{Start: bits.FromShortHexP(hashRange[0]), End: bits.FromShortHexP(hashRange[1])}
		conf.HashRange = &r
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
