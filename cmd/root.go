package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type Config struct {
	AwsID        string `json:"aws_id"`
	AwsSecret    string `json:"aws_secret"`
	BucketRegion string `json:"bucket_region"`
	BucketName   string `json:"bucket_name"`
	DBConn       string `json:"db_conn"`
}

var GlobalConfig Config

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "reflector",
	Short: "Reflector accepts blobs, stores them securely, and hosts them on the network",
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
