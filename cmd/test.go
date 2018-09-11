package cmd

import (
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "test",
		Short: "Test things",
		Run:   testCmd,
	}
	rootCmd.AddCommand(cmd)
}

func testCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	if err != nil {
		log.Fatal(err)
	}

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	combo := store.NewDBBackedS3Store(s3, db)

	values, err := reflector.BlockedSdHashes()
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range values {
		if v.Err != nil {
			continue
		}

		has, err := db.HasBlob(v.Value)
		if err != nil {
			log.Error(err)
			continue
		}

		if !has {
			continue
		}

		err = combo.Delete(v.Value)
		if err != nil {
			log.Error(err)
		}

		err = db.Block(v.Value)
		if err != nil {
			log.Error(err)
		}
	}
}
