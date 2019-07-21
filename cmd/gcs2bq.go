package cmd

import (
	"github.com/spf13/cobra"
)

// gcs2bqCmd represents the gcs2bq command
var gcs2bqCmd = &cobra.Command{
	Use:   "gcs2bq",
	Short: "restore a gcs backup to bq",
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
		plan.execute(planTypeRestore)
	},
}

func init() {
	rootCmd.AddCommand(gcs2bqCmd)
}
