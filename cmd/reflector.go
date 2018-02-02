package cmd

import (
	"log"
	"strconv"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/spf13/cobra"
)

func init() {
	var reflectorCmd = &cobra.Command{
		Use:   "reflector",
		Short: "Run reflector server",
		Run:   reflectorCmd,
	}
	RootCmd.AddCommand(reflectorCmd)
}

func reflectorCmd(cmd *cobra.Command, args []string) {
	db := new(db.SQL)
	err := db.Connect(GlobalConfig.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(GlobalConfig.AwsID, GlobalConfig.AwsSecret, GlobalConfig.BucketRegion, GlobalConfig.BucketName)
	combo := store.NewDBBackedS3Store(s3, db)
	log.Fatal(reflector.NewServer(combo).ListenAndServe("localhost:" + strconv.Itoa(reflector.DefaultPort)))
}
