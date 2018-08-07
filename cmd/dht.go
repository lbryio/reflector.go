package cmd

import (
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/dht/bits"

	"github.com/spf13/cobra"
)

var dhtNodeID string
var dhtPort int
var dhtRpcPort int
var dhtSeeds []string

func init() {
	var cmd = &cobra.Command{
		Use:       "dht [connect|bootstrap]",
		Short:     "Run dht node",
		ValidArgs: []string{"connect", "bootstrap"},
		Args:      argFuncs(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Run:       dhtCmd,
	}
	cmd.PersistentFlags().StringVar(&dhtNodeID, "nodeID", "", "nodeID in hex")
	cmd.PersistentFlags().IntVar(&dhtPort, "port", 4567, "Port to start DHT on")
	cmd.PersistentFlags().IntVar(&dhtRpcPort, "rpcPort", 0, "Port to listen for rpc commands on")
	cmd.PersistentFlags().StringSliceVar(&dhtSeeds, "seeds", []string{}, "Addresses of seed nodes")
	rootCmd.AddCommand(cmd)
}

func dhtCmd(cmd *cobra.Command, args []string) {
	if args[0] == "bootstrap" {
		node := dht.NewBootstrapNode(bits.Rand(), 1*time.Millisecond, 1*time.Minute)
		listener, err := net.ListenPacket(dht.Network, "127.0.0.1:"+strconv.Itoa(dhtPort))
		checkErr(err)
		conn := listener.(*net.UDPConn)
		err = node.Connect(conn)
		checkErr(err)
		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
		<-interruptChan
		node.Shutdown()
	} else {
		nodeID := bits.Rand()
		if dhtNodeID != "" {
			nodeID = bits.FromHexP(dhtNodeID)
		}
		log.Println(nodeID.String())

		dhtConf := dht.NewStandardConfig()
		dhtConf.Address = "0.0.0.0:" + strconv.Itoa(dhtPort)
		dhtConf.RPCPort = dhtRpcPort
		if len(dhtSeeds) > 0 {
			dhtConf.SeedNodes = dhtSeeds
		}

		d := dht.New(dhtConf)
		d.Start()

		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		<-interruptChan
		d.Shutdown()
	}
}
