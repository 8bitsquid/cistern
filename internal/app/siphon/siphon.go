package siphon

import (

	"github.com/Jeffail/tunny"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cistern"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/naptime"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/surveyor"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"go.uber.org/zap"
)

const (
	MAX_JOBS = 3
	DRAIN_ON_START = true

	CONFIG_KEY_MAX_JOBS = "siphon.max_jobs"
	CONFIG_KEY_DRAIN_ON_START = "siphone.drain_on_start"

)

func init(){
	viper.SetDefault(CONFIG_KEY_MAX_JOBS, MAX_JOBS)
	viper.SetDefault(CONFIG_KEY_DRAIN_ON_START, DRAIN_ON_START)
}

type SiphonWorker tunny.Worker

type Siphon struct {
	surveyor *surveyor.Surveyor
	cistern *cistern.Cistern
	cache *cache.Cache
	napTime *naptime.Naptime

	done chan struct{}
	
	watcher *fsnotify.Watcher
	drainOnStart bool
}

// TODO: jeeeeez fix this - oof
func NewSiphon(surveyor *surveyor.Surveyor, cistern *cistern.Cistern, cache *cache.Cache) *Siphon {

	s := &Siphon{
		surveyor: surveyor,
		cistern: cistern,
		cache: cache,
		drainOnStart: true,
	}
	//Setup file system watcher
	var err error
	s.watcher, err = fsnotify.NewWatcher()
	logger.PanicCheck(err)

	return s
}

func (s *Siphon) Start(baseDir string, done chan struct{}) error {

	s.watcher.Add(baseDir)
	s.surveyor.Start(done)
	go func ()  {
		for {
			select {
			case event, ok := <-s.watcher.Events:
				if !ok {
					return
				}
				s.handleCacheEvent(event)

			case <-done:
				s.watcher.Close()
			}
		}
	}()

	return nil
}

func (s *Siphon) Intake(path... string) {
	for _, p := range path {
		zap.S().Debugf("siphoning to cistern: %s", p)
		s.cistern.StoreData(p)
	}
}

func (s *Siphon) Drain() error {

	cache, err := s.surveyor.FlushCache()
	logger.PanicCheck(err)

	for _, c := range cache {
		s.handleCache(c.Name())
	}

	return nil
}

func (s *Siphon) handleCache(path string) {

	ct := cacheType(path)
	switch ct {
	case STATE:
		return
	case RECORD:
		s.Intake(path)
		return
	case METADATA:
		s.Intake(path)
		return

	}

	info, err := s.cache.Stat(path)
	logger.PanicCheck(err)

	if info.IsDir() {
		err := s.watcher.Add(info.Name())
		if err != nil {
			zap.S().Errorw("unable to attach siphon watcher to dir", "path", info, "error", err)
		}
		return
	}

	zap.S().Debugf("skipping unknown cache type: %s", path)
}

func (s *Siphon) handleCacheEvent(event fsnotify.Event) {
	
	switch {
	case event.Op&fsnotify.Create == fsnotify.Create:
		s.handleCache(event.Name)
	case event.Op&fsnotify.Write == fsnotify.Write:
		zap.S().Debugw("write to cache detected", "event", event)
		return
	case event.Op&fsnotify.Remove == fsnotify.Remove:
		s.handleCacheRemove(event.Name)
		break

	case event.Op&fsnotify.Rename == fsnotify.Rename:
		zap.S().Debugw("rename to cache detected", "event", event)
		break

	case event.Op&fsnotify.Chmod == fsnotify.Chmod:
		zap.S().Debugw("chmod to cache detected", "event", event)
		break
	}
}

func (s *Siphon) handleCacheRemove(path string) {

	info, err := s.cache.Stat(path)
	logger.PanicCheck(err)

	if info.IsDir() {
		s.watcher.Remove(path)
		zap.S().Debugf("removed file watcher from %s", path)
		return
	}

	ct := cacheType(path)
	switch ct {
	case STATE:
		err := s.cache.DeleteFile(path)
		if err != nil {
			zap.S().Errorw("unable to remove state", "path", path, "error", err)
			return
		}
		zap.S().Debugf("state file removed: %s", path)
		return
	case RECORD:
		err := s.cache.DeleteFile(path)
		if err != nil {
			zap.S().Errorw("unable to remove cache path", "path", path, "error", err)
			return
		}
		zap.S().Debugf("record file removed: %s", path)
		return
	case METADATA:
		err := s.cache.DeleteFile(path)
		if err != nil {
			zap.S().Errorw("unable to remove metadata", "path", path, "error", err)
			return
		}
		zap.S().Debugf("metadata file removed: %s", path)
		return
	default:
		zap.S().Warnf("cannot remove unknown cache object: %v", path)
	}
}
