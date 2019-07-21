# bqcp

`bqcp` is a command line tool for backing up and restoring entire bigquery projects. It supports a basic command to export a project into Google Cloud Storage and vice versa. 

## usage
export bq to gcs:
```bash
./bqcp bq2gcs --project source_project --bucket source_project_bq_backups
```

restore from gcs to bq:
```bash
./bqcp bq2gcs --project target_project --bucket target_project_bq_backups
```

## log file
`bqcp` logs info and error logs to bqcp.log and to the console, so you can easily track the progress of the backup and restore operations. 

## complete list of Google BigQuery's export limitations

- You cannot export table data to a local file, to Google Sheets, or to Google Drive. The only supported export location is Cloud Storage. For information on saving query results, see Downloading and saving query results.
- You can export up to 1 GB of table data to a single file. If you are exporting more than 1 GB of data, use a wildcard to export the data into multiple files. When you export data to multiple files, the size of the files will vary.
- You cannot export nested and repeated data in CSV format. Nested and repeated data is supported for Avro and JSON exports.
- When you export data in JSON format, INT64 (integer) data types are encoded as JSON strings to preserve 64-bit precision when the data is read by other systems.
- You cannot export data from multiple tables in a single export job.
- When you export data from partitioned tables, you cannot export individual partitions.
- You cannot choose a compression type other than GZIP when you export data using the GCP Console or the classic BigQuery web UI.
- Load jobs per table per day — 1,000 (including failures)
- Load jobs per project per day — 100,000 (including failures)


## TODO
- Support backing up and restoring ML models.  We currently save the schema but do not restore ML models. It looks like we'll have to use CREATE MODEL query (no support for creating models via the official go bq client at the moment) (see [Model](https://godoc.org/cloud.google.com/go/bigquery#Dataset.Model)).
- Improve finding out if a table exists (currently we catch "already exists" error)# bqcp
