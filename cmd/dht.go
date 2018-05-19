package cmd

import (
	"github.com/lbryio/reflector.go/dht"

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
	conf := &dht.Config{
		Address: "0.0.0.0:4460",
		SeedNodes: []string{
			"34.231.152.182:4460",
		},
	}

	d, err := dht.New(conf)
	checkErr(err)

	err = d.Start()
	checkErr(err)
	defer d.Shutdown()

	err = d.Ping("34.231.152.182:4470")
	checkErr(err)

	err = d.Announce(dht.RandomBitmapP())
	checkErr(err)

	d.PrintState()
}
