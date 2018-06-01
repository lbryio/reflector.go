package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/lbryio/lbry.go/errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Config is the base configuration for Prism when no sub commands are used.
type Config struct {
	AwsID        string `json:"aws_id"`
	AwsSecret    string `json:"aws_secret"`
	BucketRegion string `json:"bucket_region"`
	BucketName   string `json:"bucket_name"`
	DBConn       string `json:"db_conn"`
}

var verbose bool
var conf string
var globalConfig Config

var rootCmd = &cobra.Command{
	Use:   "reflector",
	Short: "Reflector accepts blobs, stores them securely, and hosts them on the network",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if verbose {
			log.SetLevel(log.DebugLevel)
		}

		var err error
		if conf == "" {
			log.Errorln("--conf flag required")
			os.Exit(1)
		} else {
			globalConfig, err = loadConfig(conf)
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
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")
	rootCmd.PersistentFlags().StringVar(&conf, "conf", "config.json", "Path to config")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
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
