package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/lbryio/reflector.go/config"
	"github.com/lbryio/reflector.go/reflector"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var uploadWorkers int
var uploadSkipExistsCheck bool
var uploadDeleteBlobsAfterUpload bool

func init() {
	var cmd = &cobra.Command{
		Use:   "upload PATH",
		Short: "Upload blobs to S3",
		Args:  cobra.ExactArgs(1),
		Run:   uploadCmd,
	}
	cmd.PersistentFlags().IntVar(&uploadWorkers, "workers", 1, "How many worker threads to run at once")
	cmd.PersistentFlags().BoolVar(&uploadSkipExistsCheck, "skipExistsCheck", false, "Dont check if blobs exist before uploading")
	cmd.PersistentFlags().BoolVar(&uploadDeleteBlobsAfterUpload, "deleteBlobsAfterUpload", false, "Delete blobs after uploading them")
	rootCmd.AddCommand(cmd)
}

func uploadCmd(cmd *cobra.Command, args []string) {
	store, err := config.LoadStores(conf, "upload")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Shutdown()

	databaseConn, err := config.LoadDatabase(conf, "upload")
	if err != nil {
		log.Fatal(err)
	}

	uploader := reflector.NewUploader(databaseConn, store, uploadWorkers, uploadSkipExistsCheck, uploadDeleteBlobsAfterUpload)

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interruptChan
		uploader.Stop()
	}()

	err = uploader.Upload(args[0])
	checkErr(err)
}
