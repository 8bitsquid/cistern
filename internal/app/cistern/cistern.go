package cistern

import (
	"github.com/Jeffail/tunny"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/naptime"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/restic"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	BATCH_SIZE = "20"
	MAX_JOBS = "1"

	CONFIG_KEY_BATCH_SIZE = "cistern.batch_size"
	CONFIG_KEY_MAX_JOBS = "cistern.max_jobs"
	CONFIG_KEY_STORAGE = "s3"
)

func init(){
	viper.SetDefault(CONFIG_KEY_BATCH_SIZE, BATCH_SIZE)
	viper.SetDefault(CONFIG_KEY_MAX_JOBS, MAX_JOBS)
}

type BackupRequest struct {
	path string
	tags []string
}

type BatchRequest struct {
	backups []BackupRequest
	storage *restic.S3
}

type Cistern struct {
	storage *restic.S3
	cache *cache.Cache

	batchSize int
	batch []BackupRequest

	backupRequests chan BackupRequest
	Workers *tunny.Pool
	maxWorkers int
}

func NewCistern(cache *cache.Cache, naptime *naptime.Naptime) *Cistern {
	c := &Cistern {
		cache: cache,
		backupRequests: make(chan BackupRequest),
	}
	c.UpdateSettings()

	// Setup storage
	storageConfig := restic.S3Config{}
	err := viper.UnmarshalKey(CONFIG_KEY_STORAGE, &storageConfig)
	logger.PanicCheck(err)

	c.storage, err = restic.NewS3(&storageConfig)
	logger.PanicCheck(err)

	c.Workers = tunny.NewFunc(c.maxWorkers, func(i interface{}) interface{} {
		return ProcessBackupBatch(i)
	})

	naptime.AddWorkerPool("Cistern Workers", c.Workers, c.maxWorkers)

	return c
}

func (c *Cistern) StoreData(path string, tags... string) {
	br := BackupRequest{
		path: path, 
		tags: tags,
	}
	c.batch = append(c.batch, br)
	if len(c.batch) >= c.batchSize {
		c.doBatch()
	}
}

func (c *Cistern) doBatch() error {
	//shift leading batch off the batch map
	s := c.batchSize
	var b []BackupRequest
	b, c.batch = c.batch[:s], c.batch[s:]

	br := BatchRequest{
		backups: b,
		storage: c.storage,
	}
	err := c.Workers.Process(br)
	if err != nil {
		// if error processing batch, push back into the backup queue
		c.batch = append(b, c.batch...)
		return err.(error)
	}

	c.cleanBatch(b)
	return nil
}

func (c *Cistern) cleanBatch(b []BackupRequest) {
	for _, cacheItem := range b {
		err := c.cache.DeleteFile(cacheItem.path)
		if err != nil {
			zap.S().Errorw("unable to delete cache item", "path", cacheItem, "error", err)
		}
	}
}

func (c *Cistern) UpdateSettings() {
	c.batchSize = viper.GetInt(CONFIG_KEY_BATCH_SIZE)
	c.maxWorkers = viper.GetInt(CONFIG_KEY_MAX_JOBS)
}

// Returning nil means the backup was successful
func ProcessBackupBatch(i interface{}) (er interface{}) {
	defer func(){
		if e := recover(); e != nil {
			zap.S().Errorw("unable to backup batch", "error", e)
			er = e
		}
	}()

	batch := i.(BatchRequest)

	paths := make([]string, 0)
	args := make([]string, 0)
	for _, b := range batch.backups {
		paths = append(paths, b.path)
		args = tools.StringSliceWeave(restic.CMD_ARG_TAG, b.tags, tools.SHUTTLE_RIGHT)
	}

	cmd := make([]string, 0)
	cmd = append(cmd, restic.CMD_BACKUP)
	cmd = append(cmd, args...)
	cmd = append(cmd, paths...)
	_, err := batch.storage.RunCmd(cmd...)
	if err != nil {
		zap.S().Errorw("unable to backup batch", "error", err)
		return err
	}

	return nil
}

