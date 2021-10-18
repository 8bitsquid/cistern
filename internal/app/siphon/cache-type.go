package siphon

import (
	"path/filepath"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/cache"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/app/surveyor"
)

type CacheType int

const (
	METADATA CacheType = iota
	RECORD
	STATE
	UNKNOWN
)

func cacheType(path string) CacheType {
	_, file := filepath.Split(path)

	if file == surveyor.METADATA_FILE_NAME {
		return METADATA
	}
	if filepath.Ext(file) == cache.EXT_CSV {
		return RECORD
	}
	if file == cache.STATE_FILE {
		return STATE
	}

	return UNKNOWN
}