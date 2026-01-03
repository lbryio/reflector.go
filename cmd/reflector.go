package cmd

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/config"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/reflector"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	//port configuration
	receiverPort int
	metricsPort  int

	//flags configuration
	disableBlocklist bool
)

func init() {
	var cmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}

	cmd.Flags().IntVar(&metricsPort, "metrics-port", 2112, "The port reflector will use for prometheus metrics")
	cmd.Flags().IntVar(&receiverPort, "receiver-port", 5566, "The port reflector will receive content from")
	cmd.Flags().BoolVar(&disableBlocklist, "disable-blocklist", false, "Disable blocklist watching/updating")

	rootCmd.AddCommand(cmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	store, err := config.LoadStores(conf, "reflector")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Shutdown()

	servers, err := config.LoadServers(store, conf, "reflector")
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

	reflectorServer := reflector.NewIngestionServer(store)
	reflectorServer.Timeout = 3 * time.Minute
	reflectorServer.EnableBlocklist = !disableBlocklist
	err = reflectorServer.Start(":" + strconv.Itoa(receiverPort))
	if err != nil {
		log.Fatal(err)
	}
	defer reflectorServer.Shutdown()

	metricsServer := metrics.NewServer(":"+strconv.Itoa(metricsPort), "/metrics")
	metricsServer.Start()
	defer metricsServer.Shutdown()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
}
