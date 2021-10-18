package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	BULKV2_END_POINT = "jobs/query"

	// Job Status
	STATUS_UPLOAD_COMPLETE = "UploadComplete" // All job data has been uploaded and the job is ready to be processed. Salesforce has put the job in the queue.
	STATUS_IN_PROGRESS     = "InProgress"     //Salesforce is processing the job.
	STATUS_ABORTED         = "Aborted"        // The job has been aborted.
	STATUS_JOB_COMPLETE    = "JobComplete"    // Salesforce has finished processing the job.
	STATUS_FAILED          = "Failed"         // The job failed.

	URL_PARAM_JOB_TYPE = "jobType"
)

// doBulkV2Request acts as a simple middleware to make http requests to the client
func doBulkV2Request(req *http.Request, c client.Client) (*http.Response, error) {
	req = applyAPIRequest(req)
	return doAPIRequest(req, c)
}

// applyAPIRequest prepends the BulkV2 API URI to the http.Request URL.
// This is an optional convenience for API endpoints.
func applyAPIRequest(req *http.Request) *http.Request {

	// prepend BulkV2 endpoint and set headers
	req.URL.Path = path.Join(BULKV2_END_POINT, req.URL.String())
	req.Header.Set("Content-Type", "application/json")

	return req
}

// QueryJob represents a Salesforce BulkV2 Job as a Go struct
type QueryJob struct {
	ID                     string  `json:"id,omitempty"`
	Operation              string  `json:"operation,omitempty"`
	Object                 string  `json:"object,omitempty"`
	CreatedById            string  `json:"createdById,omitempty"`
	CreatedDate            string  `json:"createdDate,omitempty"`
	SystemModstamp         string  `json:"systemModstamp,omitempty"`
	State                  string  `json:"state,omitempty"`
	ConcurrencyMode        string  `json:"concurrencyMode,omitempty"`
	ContentType            string  `json:"contentType,omitempty"`
	ApiVersion             float32 `json:"apiVersion,omitempty"`
	JobType                string  `json:"jobType,omitempty"`
	LineEnding             string  `json:"lineEnding,omitempty"`
	ColumnDelimiter        string  `json:"columnDelimiter,omitempty"`
	NumberRecordsProcessed int     `json:"numberRecordsProcessed,omitempty"`
	Retries                int     `json:"retries,omitempty"`
	TotalProcessingTime    int     `json:"totalProcessingTime,omitempty"`
}

func (qj QueryJob) Complete() bool {
	return qj.State == STATUS_JOB_COMPLETE
}

func (qj QueryJob) Healthy() bool {
	return qj.State == STATUS_JOB_COMPLETE || qj.State == STATUS_IN_PROGRESS
}

func (qj QueryJob) Failed() bool {
	return qj.State == STATUS_FAILED
}

func (qj QueryJob) Aborted() bool {
	return qj.State == STATUS_ABORTED
}

const (
	DEFAULT_CONTENT_TYPE     = "CSV"
	DEFAULT_COLUMN_DELIMITER = "COMMA"
	DEFAULT_LING_ENDING      = "LF"
	CREATE_OPERATION         = "query"
)

func CreateQueryJob(query string, options ...APIOption) (QueryJob, error) {
	o := &APIOptions{}
	o.requestBody = APIRequestBody{
		Operation: CREATE_OPERATION,
		Query:     query,
	}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	return createQueryJob(o)
}

func createQueryJob(o *APIOptions) (qj QueryJob, er error) {

	payload, err := json.Marshal(o.requestBody)
	logger.PanicCheck(err)

	req, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(payload))
	logger.PanicCheck(err)

	resp, err := doBulkV2Request(req, o.client)
	if err != nil {
		return QueryJob{}, err
	}

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	if err != nil {
		return QueryJob{}, err
	}

	var job QueryJob
	err = json.Unmarshal(bodyBytes, &job)
	logger.PanicCheck(err)

	return job, nil
}

// GetQueryJob returns a BulkV2 QueryJob given the jobID
func GetQueryJob(jobID string, options ...APIOption) QueryJob {
	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			zap.S().Error(e.(salesforce.SalesforceError))
		}
	}()
	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	return getQueryJob(jobID, o)
}

func getQueryJob(jobID string, o *APIOptions) QueryJob {

	req, err := http.NewRequest(http.MethodGet, jobID, nil)
	logger.PanicCheck(err)

	resp, err := doBulkV2Request(req, o.client)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	logger.PanicCheck(err)

	job := QueryJob{}
	err = json.Unmarshal(bodyBytes, &job)
	logger.PanicCheck(err)

	return job
}

type AllQueryJobs struct {
	Done    bool `mapstructure:"done"`
	Records []QueryJob `mapstructure:"records"`
	NextURL string `mapstructure:"nextRecordsUrl"`
}

// GetAllQueryJobs loops through BulkV2 Jobs pages, returning all Query Jobs
func GetAllQueryJobs(options ...APIOption) (jobs *AllQueryJobs, err error) {
	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(salesforce.SalesforceError)
		}
	}()

	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	allQueryJobs := getAllQueryJobs(o)

	return allQueryJobs, nil
}

func getAllQueryJobs(o *APIOptions) *AllQueryJobs {
	jobs := &AllQueryJobs{}
	params := url.Values{}
	for !jobs.Done {

		req, err := http.NewRequest(http.MethodGet, "", nil)
		logger.PanicCheck(err)

		if jobs.NextURL != "" {
			params.Set("queryLocator", jobs.NextURL)
			req.URL.RawQuery = params.Encode()
		}

		resp, err := doBulkV2Request(req, o.client)
		logger.PanicCheck(err)

		bodyBytes, err := tools.HTTPGetResponseBody(resp)
		logger.PanicCheck(err)

		var jobsPage AllQueryJobs
		err = json.Unmarshal(bodyBytes, &jobsPage)
		if err != nil {
			zap.S().Debugf("error unmarshalling AllQueryJobs request", "body", string(bodyBytes), "error", err)
		}

		if len(jobsPage.Records) > 0 {
			recordsLoop:
			for _, job := range jobsPage.Records {
				if len(o.filters) > 0 {
					for _, f := range o.filters {
						if f.key == "createdById" && f.val == job.CreatedById {
							if f.operation == FILTER_OPERATION_INCLUDE {
								break
							} 
							break recordsLoop
						}
					}
				} 

				jobs.Records = append(jobs.Records, job)
			}
		}
		jobs.Done = jobsPage.Done
		jobs.NextURL = jobsPage.NextURL
	}

	return jobs
}

// QueryJobResults
const (
	QUERY_JOB_RESULTS_ENDPOINT = "results"
	// TODO: move headers to salesforce.go file
	HEADER_CSV = "text/csv"

	// Results Response Headers
	HEADER_NUMBER_OF_RECORDS = "Sforce-NumberOfRecords"
	HEADER_LOCATOR           = "Sforce-Locator"
)

type QueryJobResults struct {
	JobID           string `json:"queryJobId"`
	NumberOfRecords int    `json:"maxRecords"`
	NextLocator     string `json:"locator"`
	Data            []byte
	Format          string
}

func GetQueryJobResults(jobID string, options ...APIOption) (qj QueryJobResults, err error) {
	o := &APIOptions{
		urlQueryParams: url.Values{},
	}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	return getQueryJobResults(jobID, o)
}

func getQueryJobResults(jobID string, o *APIOptions) (qj QueryJobResults, err error) {
	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(APIError)
		}
	}()

	endPoint, err := tools.URLBuilder(jobID, QUERY_JOB_RESULTS_ENDPOINT)
	logger.PanicCheck(err)

	if len(o.urlQueryParams) > 0 {
		endPoint.RawQuery = o.urlQueryParams.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, endPoint.String(), nil)
	logger.PanicCheck(err)

	req.Header.Add("Accept", HEADER_CSV)

	resp, err := doBulkV2Request(req, o.client)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	logger.PanicCheck(err)

	numRecords, err := strconv.Atoi(resp.Header.Get(HEADER_NUMBER_OF_RECORDS))
	logger.PanicCheck(err)

	nextLocator := resp.Header.Get(HEADER_LOCATOR)

	results := QueryJobResults{
		JobID:           jobID,
		NumberOfRecords: numRecords,
		NextLocator:     nextLocator,
		Data:            bodyBytes,
		Format:          DEFAULT_CONTENT_TYPE,
	}

	return results, nil
}

// AbortQueryJob
func AbortQueryJob(jobID string, options ...APIOption) (qj QueryJob, err error) {
	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(salesforce.SalesforceError)
		}
	}()
	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	return abortQueryJob(jobID, o)
}

func abortQueryJob(jobID string, o *APIOptions) (QueryJob, error) {
	state := APIRequestBody{
		State: STATUS_ABORTED,
	}
	paylod, err := json.Marshal(state)
	logger.PanicCheck(err)


	req, err := http.NewRequest(http.MethodPatch, jobID, bytes.NewBuffer(paylod))
	logger.PanicCheck(err)

	resp, err := doBulkV2Request(req, o.client)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	logger.PanicCheck(err)

	results := QueryJob{}
	err = json.Unmarshal(bodyBytes, &results)
	logger.PanicCheck(err)

	return results, nil
}

func DeleteQueryJob(jobID string, options ...APIOption) (err error) {
	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(salesforce.SalesforceError)
		}
	}()
	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	return deleteQueryJob(jobID, o)
}

func deleteQueryJob(jobID string, o *APIOptions) error {

	req, err := http.NewRequest(http.MethodDelete, jobID, nil)
	logger.PanicCheck(err)

	resp, err := doBulkV2Request(req, o.client)
	logger.PanicCheck(err)

	if resp.StatusCode != http.StatusNoContent {
		return errors.New("failed attempting to delete query job")
	}

	return nil
}
