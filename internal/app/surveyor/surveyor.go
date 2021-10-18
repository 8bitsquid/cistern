package surveyor

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/tunny"
	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/naptime"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	MAX_JOBS                   = 1
	MAX_METADATA_JOBS          = 1
	LAST_MODIFIED              = ""
	CACHE_DIR                  = "survey-cache"
	MAX_CACHE_SIZE             = "50Mb"
	MAX_CPU_PERCENT            = "85%"
	MAX_CHECK_INTERVAL         = "1m"
	MAX_DAILY_RECORDS_REQUESTS = "2000"

	CONFIG_KEY_MAX_JOBS                   = "surveyor.max_jobs"
	CONFIG_KEY_MAX_METADATA_JOBS          = "surveyor.max_jobs"
	CONFIG_KEY_LAST_MOD                   = "surveyor.last_modified"
	CONFIG_KEY_CACHE_DIR                  = "surveyor.cache_dir"
	CONFIG_KEY_MAX_CACHE_SIZE             = "surveyor.max_cache_size"
	CONFIG_KEY_MAX_CPU_PERCENT            = "surveyor.max_cpu"
	CONFIG_KEY_MAX_CHECK_INTERVAL         = "surveyor.max_check_interval"
	CONFIG_KEY_MAX_DAILY_RECORDS_REQUESTS = "surveyor.max_daily_records_requests"

	METADATA_FILE_NAME = "metadata.json"
	SURVEYOR_STATE_FILE_NAME = ".surveyor"
)

// TODO: Make interface for Sruveyor
// type Surveyor interface {
// 	Start() error
// 	Stop()
// 	RequestRecords(RecordsState)
// 	FetchRecords(RecordsState)
// 	FlushCache() ([]os.FileInfo, error)
// 	UpdateSettings()
// }

func init() {
	viper.SetDefault(CONFIG_KEY_MAX_JOBS, MAX_JOBS)
	viper.SetDefault(CONFIG_KEY_LAST_MOD, LAST_MODIFIED)
	viper.SetDefault(CONFIG_KEY_CACHE_DIR, CACHE_DIR)
	viper.SetDefault(CONFIG_KEY_MAX_CACHE_SIZE, MAX_CACHE_SIZE)
	viper.SetDefault(CONFIG_KEY_MAX_CPU_PERCENT, MAX_CPU_PERCENT)
	viper.SetDefault(CONFIG_KEY_MAX_CHECK_INTERVAL, MAX_CHECK_INTERVAL)
	viper.SetDefault(CONFIG_KEY_MAX_DAILY_RECORDS_REQUESTS, MAX_DAILY_RECORDS_REQUESTS)
}

type surveyorState struct {
	NumRecordsRequests int
}

type Surveyor struct {
	done               chan struct{}
	recordsRequest     chan string
	fetchRecords       chan RecordsState
	MetadataWorkers    *tunny.Pool
	numMetadataWorkers int

	Workers    *tunny.Pool
	numWorkers int

	client       client.Client
	cache        *cache.Cache

	maxCache                uint64
	maxCPU                  float64
	maxCheckInverval        time.Duration
	maxDailyRecordsRequests int
	lastModified            time.Time

	state surveyorState
}

func NewSurveyor(client client.Client, cache *cache.Cache, naptime *naptime.Naptime) *Surveyor {
	s := &Surveyor{
		client:         client,
		cache:          cache,
		done:           make(chan struct{}),
		recordsRequest: make(chan string),
		fetchRecords:   make(chan RecordsState),
	}
	s.state = s.getState()
	s.UpdateSettings()

	// add naptimes for worker pools
	naptime.AddWorkerPool("Surveyor Record Workers", s.Workers, s.numWorkers)
	naptime.AddWorkerPool("Surveyor Metadata Workers", s.MetadataWorkers, s.numMetadataWorkers)

	return s
}

func (s *Surveyor) Start(done chan struct{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	zap.S().Info("Starting Surveyor")
	// TODO remove before prod build
	jobs, err := api.GetAllQueryJobs(api.WithClient(s.client), api.CreatedById(s.client.GetUser()))
	zap.S().Debugf("removing existing Query Jobs: %v", len(jobs.Records))
	for _, j := range jobs.Records {
		api.DeleteQueryJob(j.ID, api.WithClient(s.client))
	}
	// get timestamp from .today state cache
	
	go func() {
		for {
			select {
			case req := <-s.recordsRequest:
				ok := RequestRecords(s, req)
				if ok {
					s.bumpNumRequests()
				}
			case fetch := <-s.fetchRecords:
				FetchRecords(s, fetch)
			case <-done:
				close(s.done)
				close(s.fetchRecords)
			}
		}
	}()

	err = DiscoverRecords(s)
	logger.PanicCheck(err)

	err = DiscoverMetadata(s)
	logger.PanicCheck(err)

	return nil
}

func (s *Surveyor) Stop() {
	close(s.done)
	close(s.recordsRequest)
	close(s.fetchRecords)
}

func (s *Surveyor) FlushCache() ([]os.FileInfo, error) {
	return s.cache.Flush()
}

func (s *Surveyor) getState() surveyorState {
	sb := s.cache.GetState(SURVEYOR_STATE_FILE_NAME)
	ss := surveyorState{}

	if len(sb) > 0 {
		err := json.Unmarshal(sb, &ss)
		if err != nil {
			zap.S().Errorw("unable to get surveyor state", "error", err)
		}
	} else {
		zap.S().Warn("surveyor state not found, assuming no previous state exists")
	}
	return ss
}

func (s *Surveyor) bumpNumRequests() {
	s.state.NumRecordsRequests = s.state.NumRecordsRequests + 1
	s.saveState()
}

func (s *Surveyor) saveState() {
	s.setState(s.state)
}

func (s *Surveyor) setState(ss surveyorState) {
	ssBytes, err := json.Marshal(ss)
	logger.PanicCheck(err)
	s.cache.SetStateWithName(SURVEYOR_STATE_FILE_NAME, ssBytes)
}

func (s *Surveyor) UpdateSettings() {
	s.numWorkers = viper.GetInt(CONFIG_KEY_MAX_JOBS)
	s.numMetadataWorkers = viper.GetInt(CONFIG_KEY_MAX_METADATA_JOBS)
	s.maxDailyRecordsRequests = viper.GetInt(CONFIG_KEY_MAX_DAILY_RECORDS_REQUESTS)

	maxCache, err := humanize.ParseBytes(viper.GetString(CONFIG_KEY_MAX_CACHE_SIZE))
	if err != nil {
		zap.S().Errorf("unable to parse maxCache config setting for Surveyor: %v", err)
	}
	s.maxCache = maxCache

	maxCPU, err := strconv.Atoi(strings.Trim(viper.GetString(CONFIG_KEY_MAX_CPU_PERCENT), "%"))
	if err != nil {
		zap.S().Error("unable to parse `max_cpu` in Surveyor config")
	}
	s.maxCPU = float64(maxCPU)

	maxCheckInverval, err := tools.ParseDuration(viper.GetString(CONFIG_KEY_MAX_CHECK_INTERVAL))
	if err != nil {
		zap.S().Errorf("unable to parse `%s` in Surveyor config", CONFIG_KEY_MAX_CHECK_INTERVAL)
	}
	s.maxCheckInverval = maxCheckInverval

	lm := viper.GetString(CONFIG_KEY_LAST_MOD)
	if lm != "" {
		lastModified, err := tools.ParseDuration(lm)
		if err != nil {
			zap.S().Warnw("unable to parse last modified in config, using default", "default", LAST_MODIFIED, "config", lm, "error", err)
			lastModified, err = time.ParseDuration(LAST_MODIFIED)
			logger.PanicCheck(err)
		}
		timeDiff := time.Now().Unix() - int64(lastModified.Seconds())
		s.lastModified = time.Unix(timeDiff, 0)
	}
}
