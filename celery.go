package gocelery

import (
	"os"
	"time"

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
var configFile, logLevel, brokerURL string
var debugMode bool

// create the worker manager
var workerManager = &WorkerManager{}

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
		viper.Set("BrokerUrl", brokerURL)
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
			workerManager.Connect()
			workerManager.Start(cmd, args)
		},
	}
	cmdWorker.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file (default is path/config.yaml|json|toml)")
	cmdWorker.PersistentFlags().BoolVarP(&debugMode, "debug", "d", false, "debug mode")
	cmdWorker.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "error", "log level, default is error. valid values: debug, info, warn, error, fatal")
	cmdWorker.PersistentFlags().StringVarP(&brokerURL, "broker-url", "b", "", "broker url")

	rootCmd.AddCommand(cmdWorker)
}

// PublishTask publishes a task to queue
func PublishTask(brokerURL string, taskName string, args []interface{}, ignoreResult bool) chan *TaskResult {
	// Initialize
	viper.SetDefault("BrokerUrl", "amqp://localhost")
	if brokerURL != "" {
		viper.Set("BrokerUrl", brokerURL)
	}
	workerManager.Connect() // connect to worker manager

	task := workerManager.PublishTask(taskName, args, nil, time.Time{}, time.Time{}, ignoreResult)
	if ignoreResult {
		log.Debug("Task Result is ignored.")
		return nil
	}
	taskResult := make(chan *TaskResult)
	go func() {
		log.Debug("Waiting for task result")
		taskResult <- workerManager.GetTaskResult(task)
		workerManager.Close() // close connetions
	}()
	return taskResult
}

// Execute starts the execution of the worker based on configurations
func Execute() {

	installCommands()
	rootCmd.Execute()
}
