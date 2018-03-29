package cmd

import (
	"github.com/lbryio/reflector.go/dht"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "dht",
		Short: "Run interactive dht node",
		Run:   dhtCmd,
	}
	RootCmd.AddCommand(cmd)
}

func dhtCmd(cmd *cobra.Command, args []string) {
	dht, err := dht.New(&dht.Config{
		Address:    "127.0.0.1:21216",
		SeedNodes:  []string{"127.0.0.1:21215"},
		PrintState: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	dht.Start()
}
