package gocelery

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	// import rabbitmq broker
	_ "github.com/taoh/gocelery/broker/rabbitmq"
)

// rootCmd is the root command, every other command needs to be attached to this command
var rootCmd = &cobra.Command{
	Use:   "gocelery",
	Short: "Gocelery is distributed task engine written in Go",
	Long:  `A fast and flexible distributed task engine with support of RabbitMQ transport`,
	Run: func(cmd *cobra.Command, args []string) {

	},
}

var rootCmdV, cmdWorker *cobra.Command
var configFile, logLevel, brokerUrl string
var debugMode bool

//Initializes flags
func init() {
	rootCmdV = rootCmd
}

func initializeConfig() {
	viper.SetConfigFile(configFile)
	//err := viper.ReadInConfig()
	log.Debug("Reading config: ", configFile)
	viper.SetDefault("BrokerUrl", "amqp://localhost")
	viper.SetDefault("LogLevel", "error")
	viper.ReadInConfig()

	if cmdWorker.PersistentFlags().Lookup("broker-url").Changed {
		viper.Set("BrokerUrl", brokerUrl)
	}
	if cmdWorker.PersistentFlags().Lookup("log-level").Changed {
		viper.Set("LogLevel", logLevel)
	}
}

func setupLogLevel() {
	log.SetOutput(os.Stderr)
	level, err := log.ParseLevel(viper.GetString("LogLevel"))
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.SetLevel(level)

	// FIXME: for debug, setting log level to debug
	// log.SetLevel(log.DebugLevel)
	log.Debug("Log Level: ", logLevel)
}

func installCommands() {
	cmdWorker = &cobra.Command{
		Use:   "worker",
		Short: "Start a worker",
		Long:  `Start workers.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Initialize
			initializeConfig()
			setupLogLevel()

			// Run worker command
			workerCmd(cmd, args)
		},
	}
	cmdWorker.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file (default is path/config.yaml|json|toml)")
	cmdWorker.PersistentFlags().BoolVarP(&debugMode, "debug", "d", false, "debug mode")
	cmdWorker.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "error", "log level, default is error. valid values: debug, info, warn, error, fatal")
	cmdWorker.PersistentFlags().StringVarP(&brokerUrl, "broker-url", "b", "", "broker url")

	rootCmd.AddCommand(cmdWorker)
}

var draining = false
var wg sync.WaitGroup

func listenToSignals() {
	//TODO: handle graceful shutdown
}

func shutdown(status int) {
	log.Debug("Shutting down")
	os.Exit(status)
}

// Execute starts the execution of the worker based on configurations
func Execute() {
	installCommands()
	go listenToSignals()
	rootCmd.Execute()
}
