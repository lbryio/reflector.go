package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/util"
	"github.com/lbryio/reflector.go/dht"

	"github.com/sirupsen/logrus"
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

var verbose []string

const (
	verboseAll        = "all"
	verboseDHT        = "dht"
	verboseNodeFinder = "nodefinder"
)

var conf string
var globalConfig Config

var rootCmd = &cobra.Command{
	Use:   "prism",
	Short: "Prism is a single entry point application with multiple sub modules which can be leveraged individually or together",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		debugLogger := logrus.New()
		debugLogger.SetLevel(logrus.DebugLevel)

		if util.InSlice(verboseAll, verbose) {
			verbose = []string{verboseDHT, verboseNodeFinder}
		}

		for _, debugType := range verbose {
			switch debugType {
			case verboseDHT:
				dht.UseLogger(debugLogger)
			case verboseNodeFinder:
				dht.NodeFinderUseLogger(debugLogger)
			}
		}

		var err error
		if conf == "" {
			logrus.Errorln("--conf flag required")
			os.Exit(1)
		} else {
			globalConfig, err = loadConfig(conf)
			if err != nil {
				logrus.Error(err)
				os.Exit(1)
			}
		}
	},

	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&verbose, "verbose", "v", []string{}, "Verbose logging for specific components")
	rootCmd.PersistentFlags().StringVar(&conf, "conf", "config.json", "Path to config")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		logrus.Errorln(err)
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
