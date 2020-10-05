package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/lbryio/lbry.go/v2/dht"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/lbryio/reflector.go/updater"

	"github.com/johntdyer/slackrus"
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
	SlackHookURL string `json:"slack_hook_url"`
	UpdateBinURL string `json:"update_bin_url"`
	UpdateCmd    string `json:"update_cmd"`
}

var verbose []string

const (
	verboseAll        = "all"
	verboseDHT        = "dht"
	verboseNodeFinder = "node_finder"
)

var conf string
var globalConfig Config

var rootCmd = &cobra.Command{
	Use:              "prism",
	Short:            "Prism is a single entry point application with multiple sub modules which can be leveraged individually or together",
	PersistentPreRun: preRun,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&verbose, "verbose", "v", []string{}, "Verbose logging for specific components")
	rootCmd.PersistentFlags().StringVar(&conf, "conf", "config.json", "Path to config. Use 'none' to disable")
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

func preRun(cmd *cobra.Command, args []string) {
	debugLogger := logrus.New()
	debugLogger.SetLevel(logrus.DebugLevel)
	debugLogger.SetOutput(os.Stderr)

	if util.InSlice(verboseAll, verbose) {
		logrus.SetLevel(logrus.DebugLevel)
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
	} else if conf != "none" {
		globalConfig, err = loadConfig(conf)
		if err != nil {
			logrus.Error(err)
			os.Exit(1)
		}
	}

	if globalConfig.SlackHookURL != "" {
		hook := &slackrus.SlackrusHook{
			HookURL:        globalConfig.SlackHookURL,
			AcceptedLevels: slackrus.LevelThreshold(logrus.InfoLevel),
			Channel:        "#reflector-logs",
			//IconEmoji:      ":ghost:",
			//Username:       "reflector.go",
		}
		//logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.AddHook(hook)
		debugLogger.AddHook(hook)
	}

	if globalConfig.UpdateBinURL != "" {
		if globalConfig.UpdateCmd == "" {
			logrus.Warnln("update_cmd is empty in conf file")
		}
		logrus.Println("starting update checker")
		go updater.Run(globalConfig.UpdateBinURL, globalConfig.UpdateCmd)
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
		return c, errors.Err(err)
	}

	err = json.Unmarshal(raw, &c)
	return c, errors.Err(err)
}

func mustGetFlagString(cmd *cobra.Command, name string) string {
	v, err := cmd.Flags().GetString(name)
	checkErr(err)
	return v
}

func mustGetFlagInt64(cmd *cobra.Command, name string) int64 {
	v, err := cmd.Flags().GetInt64(name)
	checkErr(err)
	return v
}
