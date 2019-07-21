package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	go_logger "github.com/phachon/go-logger"
	"github.com/spf13/viper"
)

var (
	cfgFile        string
	projectID      *string
	bucket         *string
	backupPath     *string
	maxErrors      *int
	maxConcurrency *int
	logger         *go_logger.Logger
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "bqcp",
	Short: "copy a bq project from/to gcs",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	defer func() {
		logger.Flush()
	}()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bqcp.yaml)")
	projectID = rootCmd.PersistentFlags().String("project", "", "Your Source GCP Project ID")
	bucket = rootCmd.PersistentFlags().String("bucket", "", "Your GCS bucket for the backup")
	backupPath = rootCmd.PersistentFlags().String("path", time.Now().Format("01-02-2006"), "optional path for the backup on GCS. By default the backup will be written to the match the project name on the root of the bucket")
	maxErrors = rootCmd.PersistentFlags().Int("max_errors", 100, "the maximum errors to allow when exporting tables (defaults to 100)")
	maxConcurrency = rootCmd.PersistentFlags().Int("max_concurrency", 50, "the maximum number of concurrent functions making BigQuery API calls (defaults to 50)")

	logger = go_logger.NewLogger()
	fileConfig := &go_logger.FileConfig{
		Filename:   "./bqcp.log",
		DateSlice:  "d",
		JsonFormat: false,
	}
	// add output to the file
	logger.Attach("file", go_logger.LOGGER_LEVEL_DEBUG, fileConfig)

	logger.Detach("console")

	// console adapter config
	consoleConfig := &go_logger.ConsoleConfig{
		Color:      true,  // Does the text display the color
		JsonFormat: false, // Whether or not formatted into a JSON string
		Format:     "",    // JsonFormat is false, logger message output to console format string
	}
	// add output to the console
	logger.Attach("console", go_logger.LOGGER_LEVEL_DEBUG, consoleConfig)

	logger.Info("bqcp application started")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".bqcp" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".bqcp")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func flagsValid() error {
	if *projectID != "" && *bucket != "" && *backupPath != "" {
		return nil
	}
	return fmt.Errorf("missing required flags")
}
