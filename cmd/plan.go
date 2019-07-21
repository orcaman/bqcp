package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	go_logger "github.com/phachon/go-logger"
	"google.golang.org/api/iterator"
)

type plan struct {
	bqClient           *bigquery.Client
	projectID          string
	DatasetDescriptors []*datasetDescriptor `json:"datasets"`
	logger             *go_logger.Logger
	infoc              chan string
	errc               chan error
	donec              chan bool
}

type datasetDescriptor struct {
	Name             string
	Metadata         *bigquery.DatasetMetadata `json:"metadata"`
	TableDescriptors []*tableDescriptor        `json:"tables"`
}

type tableDescriptor struct {
	Name     string                  `json:"name"`
	Metadata *bigquery.TableMetadata `json:"metadata"`
}

const (
	planTypeBackup = iota
	planTypeRestore
)

func newPlan(client *bigquery.Client, projectID string, logger *go_logger.Logger) *plan {
	p := &plan{
		projectID: projectID,
		bqClient:  client,
		logger:    logger,
		infoc:     make(chan string),
		errc:      make(chan error),
		donec:     make(chan bool),
	}

	go p.watchForLogsUntilDone()

	return p
}

func (p *plan) watchForLogsUntilDone() {
	t := time.Now()
	for {
		select {
		case err := <-p.errc:
			if err != nil {
				p.logger.Error(err.Error())
			} else {
				p.logger.Error("received nil error on error channel, check your error handling code")
			}
			p.logger.Flush()
		case s := <-p.infoc:
			p.logger.Info(s)
			p.logger.Flush()
		case <-p.donec:
			p.logger.Infof("backup/restore done in %f minutes", time.Since(t).Minutes())
			p.logger.Flush()
			return
		}
	}
}

func (p *plan) plan() error {
	p.infoc <- "a new plan.json is being generated..."
	ctx := context.Background()
	it := p.bqClient.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		} else {
			meta, err := p.bqClient.Dataset(dataset.DatasetID).Metadata(ctx)
			if err != nil {
				return err
			}
			dsDesc := &datasetDescriptor{
				Name:             dataset.DatasetID,
				Metadata:         meta,
				TableDescriptors: []*tableDescriptor{},
			}
			if err != nil {
				return err
			}
			ts := p.bqClient.Dataset(dataset.DatasetID).Tables(ctx)
			for {
				t, err := ts.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}

				meta, err := p.bqClient.Dataset(dataset.DatasetID).Table(t.TableID).Metadata(ctx)
				if err != nil {
					return err
				}
				tblDesc := &tableDescriptor{
					Name:     t.TableID,
					Metadata: meta,
				}
				dsDesc.TableDescriptors = append(dsDesc.TableDescriptors, tblDesc)
			}
			p.DatasetDescriptors = append(p.DatasetDescriptors, dsDesc)
		}
	}
	return nil
}

func (p *plan) execute(planType int) {
	switch planType {
	case planTypeBackup:
		p.executeBackup()
	case planTypeRestore:
		p.executeRestore()
	}
	p.donec <- true
}

func getBackupPlan() (*plan, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bkt := client.Bucket(*bucket).UserProject(*projectID)
	rc, err := bkt.Object(*backupPath + "/plan.json").NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot download plan.json from bucket %s (project %s): %v", *bucket, *projectID, err.Error())
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	var backupPlan *plan
	err = json.Unmarshal(data, &backupPlan)
	if err != nil {
		return nil, err
	}

	return backupPlan, nil
}

func (p *plan) executeRestore() {
	backupPlan, err := getBackupPlan()
	if err != nil {
		p.errc <- err
		return
	}

	p.DatasetDescriptors = backupPlan.DatasetDescriptors

	p.infoc <- fmt.Sprintf("restoring to bq project %s from plan.json found at gs://%s/%s", p.projectID, *bucket, *backupPath)
	p.concurrentRestore()
}

func (p *plan) concurrentRestore() {
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	var sem = make(chan struct{}, *maxConcurrency)

	for _, d := range p.DatasetDescriptors {
		err := p.createDataset(ctx, d)
		if err != nil {
			p.errc <- err
			return
		}
		for _, t := range d.TableDescriptors {
			wg.Add(1) // add an item to be waited for
			select {
			case sem <- struct{}{}: // try pushing a new task to the channel
				p.infoc <- fmt.Sprintf("restoring: %s.%s.%s", p.projectID, d.Name, t.Name)
				go func(ctx context.Context, datasetID string, tableName string, meta *bigquery.TableMetadata) {
					defer wg.Done()          // another one done
					defer func() { <-sem }() // release place in queue
					switch meta.Type {
					case "TABLE":
						err = p.loadNestedJSONWithSchema(ctx, datasetID, tableName, meta)
					case "VIEW":
						err = createView(ctx, p.bqClient, datasetID, tableName, meta.ViewQuery)
					}
					if err != nil {
						if strings.Contains(err.Error(), "Already Exists") {
							p.infoc <- fmt.Sprintf("skip: %s.%s.%s", p.projectID, datasetID, tableName)
						} else {
							p.errc <- fmt.Errorf("error restoring %s/%s: %v", datasetID, tableName, err)
						}
						return
					}
					p.infoc <- fmt.Sprintf("done: %s.%s.%s", p.projectID, datasetID, tableName)
				}(ctx, d.Name, t.Name, t.Metadata)
			}
		}
	}

	wg.Wait()
}

func (p *plan) createDataset(ctx context.Context, d *datasetDescriptor) error {
	if d == nil {
		return fmt.Errorf("dataset is nil")
	}
	if d.Metadata == nil {
		return fmt.Errorf("missing metadata for dataset %s", d.Name)
	}

	meta := &bigquery.DatasetMetadata{
		Location:    d.Metadata.Location,
		Description: d.Metadata.Description,
	}
	if err := p.bqClient.Dataset(d.Name).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "Error 409") {
		return err
	}
	return nil
}

func (p *plan) loadNestedJSONWithSchema(ctx context.Context, datasetID string, tableID string, meta *bigquery.TableMetadata) error {
	dataPath := "gs://" + *bucket + "/" + *backupPath + "/" + datasetID + "/" + tableID + "/data-*.json"
	gcsRef := bigquery.NewGCSReference(dataPath)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = meta.Schema

	loader := p.bqClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)

	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("Job completed with error: %v", status.Err())
	}

	return nil
}

func (p *plan) executeBackup() {
	p.infoc <- "preparing new/existing backup..."
	if err := verifyBucket(p.projectID); err != nil {
		p.errc <- err
		return
	}

	backupPlan, err := getBackupPlan()
	if err != nil {
		err = p.plan()
		if err != nil {
			p.errc <- err
			return
		}
		err = p.writePlanFile()
		if err != nil {
			p.errc <- err
			return
		}
		p.infoc <- fmt.Sprintf("successfully saved a new plan.json to: https://console.cloud.google.com/storage/browser/_details/%s/%s/plan.json?project=%s", *bucket, *backupPath, *projectID)
	} else {
		p.infoc <- "restored previous backup from plan.json"
		p.DatasetDescriptors = backupPlan.DatasetDescriptors
	}

	p.concurrentBackup()
}

func (p *plan) concurrentBackup() {
	ctx := context.Background()
	bkt, err := getBucketHandle(p.projectID)
	if err != nil {
		p.errc <- err
		return
	}

	wg := &sync.WaitGroup{}
	var sem = make(chan struct{}, *maxConcurrency)

	for _, d := range p.DatasetDescriptors {
		for _, t := range d.TableDescriptors {
			wg.Add(1) // add an item to be waited for
			select {
			case sem <- struct{}{}:
				p.infoc <- fmt.Sprintf("backing up: %s.%s.%s", p.projectID, d.Name, t.Name)
				go func(d *datasetDescriptor, t *tableDescriptor) {
					defer wg.Done()          // another one done
					defer func() { <-sem }() // release place in queue
					partFile := *backupPath + "/" + d.Name + "/" + t.Name + "/data-000000000000.json"
					if fileExistsOnGCS(ctx, partFile, bkt) {
						p.infoc <- fmt.Sprintf("skipped: %s.%s.%s", p.projectID, d.Name, t.Name)
						return
					}
					dataPath := "gs://" + *bucket + "/" + *backupPath + "/" + d.Name + "/" + t.Name + "/data-*.json"
					err = extractBigQueryTableToGCS(ctx, p.bqClient, dataPath, p.projectID, d.Name, t.Name)
					if err != nil {
						p.errc <- fmt.Errorf("error: %s.%s.%s - %v", p.projectID, d.Name, t.Name, err)
						return
					}
					p.infoc <- fmt.Sprintf("done: %s.%s.%s", p.projectID, d.Name, t.Name)

				}(d, t)
			}
		}
	}
	wg.Wait()
}

func (p *plan) writePlanFile() error {
	bkt, err := getBucketHandle(p.projectID)
	if err != nil {
		return err
	}

	ctx := context.Background()
	wc := bkt.Object(*backupPath + "/plan.json").NewWriter(ctx)
	data, err := json.Marshal(p)
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
