package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lbryio/reflector.go/config"
	"github.com/lbryio/reflector.go/internal/metrics"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var ()

func init() {
	var cmd = &cobra.Command{
		Use:   "blobcache",
		Short: "Run blobcache server",
		Run:   blobcacheCmd,
	}

	cmd.Flags().IntVar(&tcpPeerPort, "tcp-peer-port", 5567, "The port reflector will distribute content from for the TCP (LBRY) protocol")
	cmd.Flags().IntVar(&http3PeerPort, "http3-peer-port", 5568, "The port reflector will distribute content from over HTTP3 protocol")
	cmd.Flags().IntVar(&httpPeerPort, "http-peer-port", 5569, "The port reflector will distribute content from over HTTP protocol")
	cmd.Flags().IntVar(&receiverPort, "receiver-port", 5566, "The port reflector will receive content from")
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for prometheus metrics")

	cmd.Flags().BoolVar(&disableBlocklist, "disable-blocklist", false, "Disable blocklist watching/updating")
	cmd.Flags().IntVar(&requestQueueSize, "request-queue-size", 200, "How many concurrent requests from downstream should be handled at once (the rest will wait)")
	cmd.Flags().StringVar(&upstreamEdgeToken, "upstream-edge-token", "", "token used to retrieve/authenticate protected content")

	rootCmd.AddCommand(cmd)
}

func blobcacheCmd(cmd *cobra.Command, args []string) {
	store, err := config.LoadStores("blobcache.yaml")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Shutdown()

	servers, err := config.LoadServers(store, "blobcache.yaml")
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range servers {
		err = s.Start()
		if err != nil {
			log.Fatal(err)
		}
		defer s.Shutdown()
	}

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()
	defer metricsServer.Shutdown()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
}
