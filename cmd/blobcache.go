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

func init() {
	var cmd = &cobra.Command{
		Use:   "blobcache",
		Short: "Run blobcache server",
		Run:   blobcacheCmd,
	}

	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for prometheus metrics")
	cmd.Flags().BoolVar(&disableBlocklist, "disable-blocklist", false, "Disable blocklist watching/updating")

	rootCmd.AddCommand(cmd)
}

func blobcacheCmd(cmd *cobra.Command, args []string) {
	store, err := config.LoadStores(conf, "blobcache")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Shutdown()

	servers, err := config.LoadServers(store, conf, "blobcache")
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
