package app

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cistern"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/naptime"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/siphon"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/surveyor"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	BASE_DIR      = "salesforce-backups"
	FILE_MODE     = 0755
	CACHE_TIMEOUT = "30s"
	MAX_CPU_PERCENT = "85%"
	MAX_MEM_PERCENT = "85%"
	MAX_CACHE_SIZE = "2GB"

	CONFIG_KEY_BASE_DIR     = "base_dir"
	CONFIG_KEY_CACHE_TIMEOUT = "cache_timeout"
	CONFIG_KEY_MAX_CPU_PERCENT = "max_cpu_percent"
	CONFIG_KEY_MAX_MEM_PERCENT = "max_meme_percent"
	CONFIG_KEY_MAX_CACHE_SIZE = "max_disk_usage"

	EXT_CSV  = "csv"
	EXT_JSON = "json"
)

var (
	cacheTimeout time.Duration
	baseDir string
	maxCPU float64
	maxMem uint64
	maxCache uint64
)

func init() {
	// Set global config defaults
	viper.SetDefault(CONFIG_KEY_BASE_DIR, BASE_DIR)
	viper.SetDefault(CONFIG_KEY_CACHE_TIMEOUT, CACHE_TIMEOUT)
	viper.SetDefault(CONFIG_KEY_MAX_CPU_PERCENT, MAX_CPU_PERCENT)
	viper.SetDefault(CONFIG_KEY_MAX_CACHE_SIZE, MAX_CACHE_SIZE)
	viper.SetDefault(CONFIG_KEY_MAX_MEM_PERCENT, MAX_MEM_PERCENT)
}

func Start() {
	// Read config file
	viper.SetConfigFile("test.yml")
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stdout, "Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Printf("Errors: %v", err)
	}

	// Init logger - log format and set zap logging as global
	logger.InitLogger()

	settings := viper.AllSettings()
	zap.S().Debugw("Config file set", "settings", settings)

	UpdateSettings()

	// Load Salesforce details from config
	sf, err := salesforce.NewSession()
	logger.PanicCheck(err)


	// setup done channel for all parts of the app
	done := make(chan struct{})
	cache := cache.NewCache(done, baseDir, cacheTimeout)

	//if no present state in root cache dir, then set one
	// the timestamp in state is checked by other processes to midigate flooding external apis with requests
	stateExists := cache.Exists(".today")
	freshState := false
	if !stateExists {
		freshState = true		
	} else {
		tdBytes := cache.GetState(".today")
		state := string(tdBytes)
		sTime, err := time.Parse(time.UnixDate, state)
		if err != nil {
			zap.S().Warnw("unable to parse .today timestamp cache, assuming non exists", "error", err, "state", state)
			freshState = true
		}

		now := time.Now()
		hoursDiff := now.Sub(sTime)
		if hoursDiff >= 24 * time.Hour {
			freshState = true
		}
	}

	if freshState {
		cache.SetStateWithName(".today", []byte(time.Now().String()))
	}


	// Define naptimes for siphon to enfore on the surveyor and cistern
	cpuNap := naptime.NewCPUNapConditions(maxCPU)
	memNap := naptime.NewVirtMemNapConditions(maxMem)
	cacheNap := naptime.NewDiskNapConsitions(maxCache, baseDir)
	nt := naptime.NewNaptime(2 * time.Minute, cpuNap, memNap, cacheNap)

	surveyor := surveyor.NewSurveyor(sf, cache, nt)
	cistern := cistern.NewCistern(cache, nt)
	
	siphon := siphon.NewSiphon(surveyor, cistern, cache)
	siphon.Start(baseDir, done)

	// Start monitoring for naptimes
	nt.MonitorConditions()

	for {
		select {
		case <-done:
			close(done)
			zap.S().Infof("Backup session complete")
		}
	}
}

func UpdateSettings() {

	baseDir = viper.GetString(CONFIG_KEY_BASE_DIR)

	ct, err := tools.ParseDuration(viper.GetString(CONFIG_KEY_CACHE_TIMEOUT))
	if err != nil {
		zap.S().Errorf("unable to parse `%s` in Surveyor config", CONFIG_KEY_CACHE_TIMEOUT)
	}
	cacheTimeout = ct

	mcache, err := humanize.ParseBytes(viper.GetString(CONFIG_KEY_MAX_CACHE_SIZE))
	if err != nil {
		zap.S().Errorf("unable to parse maxCache config setting for Surveyor: %v", err)
	}
	maxCache = mcache

	mcpu, err := strconv.Atoi(strings.Trim(viper.GetString(CONFIG_KEY_MAX_CPU_PERCENT), "%"))
	if err != nil {
		zap.S().Error("unable to parse `max_cpu` in Surveyor config")
	}
	maxCPU = float64(mcpu)
	
	mmem, err := strconv.Atoi(strings.Trim(viper.GetString(CONFIG_KEY_MAX_MEM_PERCENT), "%"))
	if err != nil {
		zap.S().Error("unable to parse `max_cpu` in Surveyor config")
	}
	maxMem = uint64(mmem)
}
