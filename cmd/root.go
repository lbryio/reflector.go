package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/lbryio/lbry.go/errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type Config struct {
	AwsID        string `json:"aws_id"`
	AwsSecret    string `json:"aws_secret"`
	BucketRegion string `json:"bucket_region"`
	BucketName   string `json:"bucket_name"`
	DBConn       string `json:"db_conn"`
}

var Verbose bool
var Conf string
var GlobalConfig Config

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "reflector",
	Short: "Reflector accepts blobs, stores them securely, and hosts them on the network",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if Verbose {
			log.SetLevel(log.DebugLevel)
		}

		var err error
		if Conf == "" {
			log.Errorln("--conf flag required")
			os.Exit(1)
		} else {
			GlobalConfig, err = loadConfig(Conf)
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}
		}
	},

	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

func init() {
	RootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Enable verbose logging")
	RootCmd.PersistentFlags().StringVar(&Conf, "conf", "config.json", "Path to config")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		log.Errorln(err)
		os.Exit(1)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func argFuncs(funcs ...cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		for _, f := range funcs {
			err := f(cmd, args)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func loadConfig(path string) (Config, error) {
	var c Config

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, errors.Err("config file not found")
		}
		return c, err
	}

	err = json.Unmarshal(raw, &c)
	if err != nil {
		return c, err
	}

	return c, nil
}
