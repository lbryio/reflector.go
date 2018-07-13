package cmd

import (
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/dht/bits"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"math/big"
)

type NodeRPC string

type PingArgs struct {
	nodeID string
	address string
	port int
}

type PingResult string

func (n *NodeRPC) Ping(r *http.Request, args *PingArgs, result *PingResult) error {
	*result = PingResult("pong")
	return nil
}

var dhtPort int

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
		listener, err := net.ListenPacket(dht.Network, "0.0.0.0:"+strconv.Itoa(dhtPort))
		checkErr(err)
		conn := listener.(*net.UDPConn)
		err = node.Connect(conn)
		checkErr(err)
		log.Println("started node")
		node.AddKnownNode(
			dht.Contact{
			bits.FromHexP("62c8ad9fb40a16062e884a63cd81f47b94604446319663d1334e1734dcefc8874b348ec683225e4852017a846e07d94e"),
			net.ParseIP("34.231.152.182"), 4444,
			})
		_, _, err = dht.FindContacts(&node.Node, nodeID.Sub(bits.FromBigP(big.NewInt(1))), false, nil)
		rpcServer := dht.RunRPCServer(":1234", "/", node)
		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		<-interruptChan
		rpcServer.Wg.Done()
		node.Shutdown()
	}
}
