package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

func fileExistsOnGCS(ctx context.Context, path string, bkt *storage.BucketHandle) bool {
	rc, err := bkt.Object(path).NewReader(ctx)
	if err != nil {
		return false
	}
	rc.Close()
	return true
}

func createView(ctx context.Context, client *bigquery.Client, datasetID string, tableID string, viewQuery string) error {
	meta := &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	return client.Dataset(datasetID).Table(tableID).Create(ctx, meta)
}
func getBigQueryClient(projectID string) (*bigquery.Client, error) {
	ctx := context.Background()
	return bigquery.NewClient(ctx, projectID)
}

func getBucketHandle(projectID string) (*storage.BucketHandle, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return client.Bucket(*bucket).UserProject(projectID), nil
}

func extractBigQueryTableToGCS(ctx context.Context, client *bigquery.Client, path string, projectID string, datasetID string, tableID string) error {
	gcsRef := bigquery.NewGCSReference(path)
	extractor := client.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
	extractor.Location = "US"
	extractor.ExtractConfig.Dst.DestinationFormat = bigquery.JSON
	extractor.ExtractConfig.Dst.Compression = bigquery.Gzip

	job, err := extractor.Run(ctx)
	if err != nil {
		return fmt.Errorf("extractor.Run error: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait error: %v", err)
	}
	if err := status.Err(); err != nil {
		// extractor only works on tables, we save the schema for views and models but we cannot "extract" their data
		if !strings.Contains(err.Error(), "VIEW") && !strings.Contains(err.Error(), "MODEL") &&
			!strings.Contains(err.Error(), "EXTERNAL") {
			return fmt.Errorf("status.Err error: %v", err)
		}
	}

	return err
}

func verifyBucket(projectID string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bkt := client.Bucket(*bucket).UserProject(projectID)
	if err := bkt.Create(ctx, projectID, nil); err != nil && !strings.Contains(err.Error(), "Error 409") {
		return fmt.Errorf("error creating bucket %s on project %s: %s", *bucket, projectID, err.Error())
	}
	return nil
}

func extractBigQueryMetadataToGCS(
	ctx context.Context,
	client *bigquery.Client,
	bkt *storage.BucketHandle,
	path string,
	datasetID string,
	objectID string) error {
	t := client.Dataset(datasetID).Table(objectID)
	meta, err := t.Metadata(ctx)
	if err != nil {
		return err
	}
	return writeToGCS(ctx, bkt, path, meta)
}

func writeToGCS(ctx context.Context, bkt *storage.BucketHandle, path string, i interface{}) error {
	wc := bkt.Object(path).NewWriter(ctx)
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	r := bytes.NewReader(data)
	if _, err = io.Copy(wc, r); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return err
}
