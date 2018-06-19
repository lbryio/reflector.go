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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var dhtPort int

func init() {
	var cmd = &cobra.Command{
		Use:       "dht [start|bootstrap]",
		Short:     "Run dht node",
		ValidArgs: []string{"start", "bootstrap"},
		Args:      argFuncs(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Run:       dhtCmd,
	}
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
		log.Fatal("not implemented")
	}
}
