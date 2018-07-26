package cmd

import (
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/dht/bits"

	"github.com/spf13/cobra"
)

type NodeRPC string

type PingArgs struct {
	nodeID  string
	address string
	port    int
}

type PingResult string

func (n *NodeRPC) Ping(r *http.Request, args *PingArgs, result *PingResult) error {
	*result = PingResult("pong")
	return nil
}

var dhtPort int
var rpcPort int

func init() {
	var cmd = &cobra.Command{
		Use:       "dht [bootstrap|connect]",
		Short:     "Run dht node",
		ValidArgs: []string{"start", "bootstrap"},
		Args:      argFuncs(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Run:       dhtCmd,
	}
	cmd.PersistentFlags().StringP("nodeID", "n", "", "nodeID in hex")
	cmd.PersistentFlags().IntVar(&dhtPort, "port", 4567, "Port to start DHT on")
	cmd.PersistentFlags().IntVar(&rpcPort, "rpc_port", 1234, "Port to listen for rpc commands on")
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
		log.Printf("shutting down bootstrap node")
		node.Shutdown()
	} else {
		nodeIDStr := cmd.Flag("nodeID").Value.String()
		nodeID := bits.Bitmap{}
		if nodeIDStr == "" {
			nodeID = bits.Rand()
		} else {
			nodeID = bits.FromHexP(nodeIDStr)
		}
		log.Println(nodeID.String())
		node := dht.NewBootstrapNode(nodeID, 1*time.Millisecond, 1*time.Minute)
		listener, err := net.ListenPacket(dht.Network, "127.0.0.1:"+strconv.Itoa(dhtPort))
		checkErr(err)
		conn := listener.(*net.UDPConn)
		err = node.Connect(conn)
		checkErr(err)
		log.Println("started node")
		_, _, err = dht.FindContacts(&node.Node, nodeID.Sub(bits.FromBigP(big.NewInt(1))), false, nil)
		rpcServer := dht.RunRPCServer("127.0.0.1:"+strconv.Itoa(rpcPort), "/", node)
		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		<-interruptChan
		rpcServer.Wg.Done()
		node.Shutdown()
	}
}
