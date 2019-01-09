package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lbryio/lbry.go/extras/crypto"
	"github.com/lbryio/reflector.go/cluster"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:       "cluster [start|join]",
		Short:     "Start(join) to or Start a new cluster",
		ValidArgs: []string{"start", "join"},
		Args:      argFuncs(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Run:       clusterCmd,
	}
	rootCmd.AddCommand(cmd)
}

func clusterCmd(cmd *cobra.Command, args []string) {
	port := 17946
	var c *cluster.Cluster
	if args[0] == "start" {
		c = cluster.New(port, "")
	} else {
		c = cluster.New(port+1+int(crypto.RandInt64(1000)), "127.0.0.1:"+strconv.Itoa(port))
	}

	err := c.Connect()
	if err != nil {
		log.Fatal(err)
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	c.Shutdown()
}
