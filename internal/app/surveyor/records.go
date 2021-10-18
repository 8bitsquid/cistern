package surveyor

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/Jeffail/tunny"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api"
	"go.uber.org/zap"
)

var numRecordsRequests int

func init() {
	numRecordsRequests = 0
	viper.SetDefault("surveyor.find_record_attempts", 10)
}

type RecordsState struct {
	ID          string
	RequestID   string
	Done        bool
	NextLocator string
	CachePath   string
	Query       string
}

type cacheRecords struct {
	RecState RecordsState
	Data     []byte
	Cache    *cache.Cache
}

func DiscoverRecords(s *Surveyor) error {
	zap.S().Info("Searching for records")

	s.Workers = tunny.New(s.numWorkers, func() tunny.Worker {
		return &recordsWorker{
			s:             s,
			interruptChan: make(chan struct{}),
		}
	})

	queueIncompleteRecordsRequests(s)
	watchRecordsRequests(s)

	return nil
}

func RequestRecords(s *Surveyor, query string) (success bool) {
	defer func ()  {
		if e := recover(); e != nil {
			zap.S().Errorw("unable to complete records request", "error", e)
			success = false
		}
	}()


	if numRecordsRequests >= s.maxDailyRecordsRequests {
		zap.S().Warnf("max number of daily records requests reached: %d requests", s.maxDailyRecordsRequests)
		setRecordState(s.cache, RecordsState{Query: query})
		zap.S().Debugw("max dailt records requests made, caching query", "query", query)
		return false
	}
	// Create Query Job
	job, err := api.CreateQueryJob(query, api.WithClient(s.client))
	if err != nil {
		zap.S().Errorw("error creating BulkV2 Query Job with query", "query", query, "error", err)
		return false
	}
	zap.S().Infow("Query Job Created", "job", job.ID, "create_date", job.CreatedDate, "created_by_id", job.CreatedById)

	rs := RecordsState{
		ID:        job.Object,
		RequestID: job.ID,
		CachePath: job.Object,
	}
	
	setRecordState(s.cache, rs)
	return true
}

func FetchRecords(s *Surveyor, recState RecordsState) {
	done := make(chan struct{})
	defer close(done)

	// index of the Id column in the CSV

	for {
		resp, err := api.GetQueryJobResults(recState.RequestID, api.WithClient(s.client))
		logger.PanicCheck(err)
		zap.S().Debugw("records chunk recieved", "jobID", resp.JobID, "numRecords", resp.NumberOfRecords, "nextLocator", resp.NextLocator)

		if resp.NumberOfRecords == 0 {
			zap.S().Warnw("no results found for Query Job", "job", resp)
			CleanupRecords(s, recState)
			break
		}
		// process records chunk
		cr := cacheRecords{
			RecState: recState,
			Data:     resp.Data,
			Cache:    s.cache,
		}
		s.Workers.Process(cr)

		// update records stat
		recState.NextLocator = resp.NextLocator
		setRecordState(s.cache, recState)

		// Salesforce sends a string of "null", instead of a null value....
		if resp.NextLocator == "" || resp.NextLocator == "null" {
			CleanupRecords(s, recState)
			break
		}
	}
	zap.S().Infow("done getting records", "job_id", recState.RequestID, "object", recState.ID)
}

func CleanupRecords(s *Surveyor, rs RecordsState) {
	if !s.cache.Exists(rs.CachePath) {
		zap.S().Warnw("no records cache to clean, moving on", "recordsState", rs)
		return
	} else {
		err := api.DeleteQueryJob(rs.RequestID, api.WithClient(s.client))
		if err != nil {
			zap.S().Errorw("unable to delete Query Job with request id", "recordsState", rs)
		} else {
			zap.S().Infow("Query Job deleted for records", "recordState", rs)
		}
		
		err = s.cache.DeleteAll(rs.CachePath)
		if err != nil {
			zap.S().Errorw("unable to delete records cache", "recordsState", rs)
			return
		} else {
			zap.S().Infow("records cache deleted", "recodsState", rs)
		}
	}
}

func queueIncompleteRecordsRequests(s *Surveyor) {

	cacheStatePaths, err := s.cache.FindAll(cache.STATE_FILE)
	if err != nil {
		zap.S().Errorw("unable to find cache states - assuming none exist, moving on", "error", err)
		return
	}

	numCachPaths := len(cacheStatePaths)
	if numCachPaths == 0 {
		zap.S().Warn("no incomplete records requests found")
		return
	} else {
		zap.S().Infof("number of incomplete records requests found: ", numCachPaths)
	}

	cacheStates := make([]RecordsState, 0)
	for _, p := range cacheStatePaths {
		rs := getRecordState(s.cache, p)
		cacheStates = append(cacheStates, rs)
	}

	// Sort proprity in order of `Done` > `NextLocator` > `RequestID`
	sort.Slice(cacheStates, func(i, j int) bool {
		if cacheStates[i].Done && !cacheStates[j].Done {
			return true
		}
		if !cacheStates[i].Done && cacheStates[j].Done {
			return false
		}
		return cacheStates[i].NextLocator > cacheStates[j].NextLocator
	})

	go func() {
		for _, state := range cacheStates {
			if state.ID == "" && state.Query != "" {
				s.recordsRequest <- state.Query
			} else {
				s.fetchRecords <- state
			}
			select {
			case <-s.done:
				return
			}
		}
	}()
}

func watchRecordsRequests(s *Surveyor) {
	userID := s.client.GetUser()

	go func() {
		for {
			recordsRequests, err := api.GetAllQueryJobs(api.WithClient(s.client), api.CreatedById(userID))
			logger.PanicCheck(err)

			for _, r := range recordsRequests.Records {
				if r.Complete() {
					s.fetchRecords <- RecordsState{
						ID: r.Object,
						RequestID: r.ID,
					}
				}
			}
			select {
			case <-time.After(s.maxCheckInverval):
			case <-s.done:
				return
			}
		}
	}()
}

func getRecordState(cache *cache.Cache, path string) RecordsState {
	state := cache.GetState(path)

	rs := RecordsState{}

	if state != nil || len(state) > 0 {
		json.Unmarshal(state, &rs)

		if rs.CachePath == "" {
			rs.CachePath = rs.ID
		}
	}

	return rs
}

func setRecordState(cache *cache.Cache, rs RecordsState) {
	if rs.CachePath == "" {
		rs.CachePath = rs.ID
	}

	state, err := json.Marshal(rs)
	if err != nil {
		zap.S().Errorw("unable to set record state", "state", rs, "error", err)
	}
	cache.SetState(rs.CachePath, state)
}

type recordsWorker struct {
	s            *Surveyor

	interruptChan chan struct{}
	terminated    bool
}

func (rw *recordsWorker) Process(i interface{}) interface{} {
	cr := i.(cacheRecords)
	defer func() {
		if e := recover(); e != nil {
			zap.S().Errorw("unabel to process CSV records batch", "csv", cr, "error", e)
		}
	}()

	cr.Cache.CacheCSV(cr.RecState.ID, cr.Data)

	return nil
}

func (w *recordsWorker) BlockUntilReady() {}

func (w *recordsWorker) Interrupt() {
	w.interruptChan <- struct{}{}
}

func (w *recordsWorker) Terminate() {
	w.terminated = true
}
