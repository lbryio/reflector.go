package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "start [cluster-address]",
		Short: "Run prism server",
		Run:   startCmd,
		Args:  cobra.RangeArgs(0, 1),
	}
	rootCmd.AddCommand(cmd)
}

func startCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	comboStore := store.NewDBBackedS3Store(s3, db)

	clusterAddr := ""
	if len(args) > 0 {
		clusterAddr = args[0]
	}

	p := reflector.NewPrism(comboStore, clusterAddr)
	err = p.Connect()
	if err != nil {
		log.Error(err)
		return
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	p.Shutdown()
}
