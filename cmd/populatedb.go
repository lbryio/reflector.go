package cmd

import (
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/store/speedwalk"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	diskStorePath string
)

func init() {
	var cmd = &cobra.Command{
		Use:   "populate-db",
		Short: "populate local database with blobs from a disk storage",
		Run:   populateDbCmd,
	}
	cmd.Flags().StringVar(&diskStorePath, "store-path", "",
		"path of the store where all blobs are cached")
	rootCmd.AddCommand(cmd)
}

func populateDbCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector %s", meta.VersionString())
	if diskStorePath == "" {
		log.Fatal("store-path must be defined")
	}
	localDb := &db.SQL{
		SoftDelete:  true,
		TrackAccess: db.TrackAccessBlobs,
		LogQueries:  log.GetLevel() == log.DebugLevel,
	}
	err := localDb.Connect("reflector:reflector@tcp(localhost:3306)/reflector")
	if err != nil {
		log.Fatal(err)
	}
	blobs, err := speedwalk.AllFiles(diskStorePath, true)
	err = localDb.AddBlobs(blobs)
	if err != nil {
		log.Errorf("error while storing to db: %s", errors.FullTrace(err))
	}
}
