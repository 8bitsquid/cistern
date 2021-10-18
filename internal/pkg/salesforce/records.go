package salesforce

// import (
// 	"context"

// 	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api/bulkv2"
// 	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api/rest"
// )

// type lastModContextKey string

// func getAllRecordsWithContext(ctx context.Context, client *Salesforce, query string, records chan<- bulkv2.QueryJobResults) {
// 	ctx, cancel := context.WithCancel(ctx)

// 	if lastMod := ctx.Value()

// 	query := bulkv2.SqolSelectFrom(sobj, bulkv2.WhereLastModifiedAfter(s.LastModified))
// 	job, err := s.Client.API.BulkV2.CreateQueryJob(query)
// 	if err != nil {
// 		zap.S().Errorw("unable to create BulkV2 query job", "sobject", sobj.Name, "error", err)
// 		return
// 	}

// 	for {
// 		zap.S().Info("Waiting 10 seconds to check BulkV2 Query Job status")
// 		time.Sleep(10 * time.Second)

// 		switch job.State {
// 		case bulkv2.STATUS_IN_PROGRESS:
// 			s.Client.API.BulkV2.UpdateQueryJob(job)
// 			continue
// 		case bulkv2.STATUS_FAILED:
// 			zap.S().Errorw("BulkV2 query job failed", "job", job)
// 			return
// 		case bulkv2.STATUS_ABORTED:
// 			zap.S().Warnw("BulkV2 query job aborted", "job", job)
// 			return
// 		case bulkv2.STATUS_JOB_COMPLETE:
// 			zap.S().Infof("Bulkv2 query job finished", "job", job)
// 			break
// 		}
// 	}

// 	// Process the BulkV2 results
// 	results, err := s.Client.API.BulkV2.QueryJobResults(job.ID)
// 	if err != nil {
// 		zap.S().Errorw("unable to get results from BulkV2 Query Job", "job", job, "error", err)
// 		return
// 	}

// 	err = s.cacheBulkCSV(dir, results.Data)
// 	if err != nil {
// 		zap.S().Errorw("unable to cache results from BulkV2 Query Job", "job_id", results.JobID, "results", string(results.Data), "error", err)
// 		return
// 	}

// 	// loop through any more results from bulk job
// 	for results.NextLocator != "" {
// 		zap.S().Infof("Getting next batch of results from BulkV2 Query Job", "job_id", results.JobID)
// 		results, err = s.Client.API.BulkV2.QueryJobResults(results.JobID, bulkv2.NextLocator(results.NextLocator))
// 		if err != nil {
// 			zap.S().Errorw("unable to get next batch of results from BulkV2 Query Job", "job", job, "error", err)
// 			break
// 		}
// 	}

// }
