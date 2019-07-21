package cmd

import (
	"github.com/spf13/cobra"
)

// bq2gcsCmd represents the bq2gcs command
var bq2gcsCmd = &cobra.Command{
	Use:   "bq2gcs",
	Short: "backup bq to gcs",
	Run: func(cmd *cobra.Command, args []string) {
		err := flagsValid()
		if err != nil {
			logger.Error(err.Error())
			return
		}

		client, err := getBigQueryClient(*projectID)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		plan := newPlan(client, *projectID, logger)
		plan.execute(planTypeBackup)
	},
}

func init() {
	rootCmd.AddCommand(bq2gcsCmd)
}
