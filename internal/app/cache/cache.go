package cache

import (
	"bufio"
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api"
	"go.uber.org/zap"
)

const (
	BASE_DIR      = "salesforce-backups"
	FILE_MODE     = 0755
	CACHE_TIMEOUT = 30 * time.Second

	CONFIG_KEY_BASE_DIR      = "base_dir"
	CONFIG_KEY_CACHE_TIMEOUT = "cache_timeout"

	EXT_CSV  = "csv"
	EXT_JSON = "json"

	CSV_HEADER_FILE_NAME = "header.csv"
)

type Cache struct {
	fs           *afero.Afero
	dir          string
	stateUpdates chan StateUpdate
}

func NewCache(done chan struct{}, dir string, timeout time.Duration) *Cache {
	c := &Cache{
		dir: dir,
		stateUpdates: make(chan StateUpdate),
	}

	if timeout == 0 {
		timeout = viper.GetDuration(CONFIG_KEY_CACHE_TIMEOUT)
	}

	// Check cache directory exists and attempt to create it if not
	err := os.MkdirAll(dir, os.FileMode(FILE_MODE))
	logger.PanicCheck(err)

	// Base file system layer, on disk and restricted to the base path
	baseLayer := afero.NewBasePathFs(afero.NewOsFs(), dir)
	// Overlay file system layer, in memory
	overlay := afero.NewMemMapFs()
	// init cache file system
	fs := afero.NewCacheOnReadFs(baseLayer, overlay, timeout)
	c.fs = &afero.Afero{
		Fs: fs,
	}

	go func() {
		for {
			select {
			case s := <-c.stateUpdates:
				switch s.operation {
				case UPDATE:
					if len(s.data) == 0 {
						zap.S().Warnf("updating state as empty for cache: %s", s.cachePath)
					}
					c.setState(s.cachePath, s.data, s.withName)
				case CLEAR:
					c.clearState(s.cachePath)
				default:
					zap.S().Warnw("invalid state operation", "cachePath", s.cachePath, "operation", s.operation)
				}
			case <-done:
				close(c.stateUpdates)
				return
			}
		}
	}()

	return c
}

// if error, assume cache doesn't exists
func (c *Cache) Exists(cachePath string) bool {
	exists, err := c.fs.Exists(cachePath)
	if err != nil {
		zap.S().Errorw("unable to check if cache path exists", "cache", cachePath, "error", err)
	}
	return exists && err != nil
}

func (c *Cache) Stat(cachePath string) (os.FileInfo, error) {
	// ensure path is exists in basdir
	relPath, err := filepath.Rel(c.dir, cachePath)
	if err != nil {
		zap.S().Warnw("relative path not found on cache, assuming it doesn't exist yet", "path", cachePath, "error", err)
	}

	return c.fs.Stat(relPath)
}

func (c *Cache) GetCacheDir() string {
	return c.dir
}

func (c *Cache) GetFileInfo(filePath string) (os.FileInfo, error) {
	return c.fs.Stat(filePath)
}

func (c *Cache) FindAll(name string) ([]string, error) {
	defer func() {
		if e := recover(); e != nil {
			zap.S().Errorw("error attempting to get first file", "file", name)
		}
	}()

	found := make([]string, 0)

	err := c.fs.Walk(".", func(path string, info fs.FileInfo, e error) error {
		defer func() {
			if e := recover(); e != nil {
				zap.S().Warnw("unable to walk through cache location", "path", path, "file_info", info, "error", e)
			}
		}()

		if info.IsDir() {
			return nil
		}

		if info.Name() == name {
			zap.S().Debugf("found cache cache file found: %s", path)
			found = append(found, path)
		}
		return nil
	})

	return found, err
}

func (c *Cache) GetState(cachePath string) []byte {
	exists, err := c.fs.Exists(cachePath)
	if err != nil {
		zap.S().Warnw("error occurred getting cache state", "cache", cachePath, "error", err)
		return nil
	}
	if !exists {
		return nil
	}

	s, err := c.fs.ReadFile(cachePath)
	if err != nil {
		zap.S().Warnw("error occurred getting cache state", "cache", cachePath, "error", err)
		return nil
	}
	return s
}

// TODO: add getting raw string state
// always return an empty string, even on errors
// to assume cache state needs to be (re)set
// func (c *Cache) GetStateString(cachePath string) string {
// 	s := c.GetState(cachePath)
// 	buf := new(strings.Builder)
// 	_, err := io.Copy(buf, s)
// 	if err != nil {
// 		zap.S().Warnw("error occurred getting cache state", "cache", cachePath, "error", err)
// 	}
// 	state := buf.String()
// 	return state

// }

func (c *Cache) SetState(cachePath string, data []byte) {
	c.stateUpdates <- StateUpdate{
		operation: UPDATE,
		cachePath: cachePath,
		data:      data,
	}
}

func (c *Cache) SetStateWithName(cachePath string, data[]byte) {
	c.stateUpdates <- StateUpdate{
		operation: UPDATE,
		cachePath: cachePath,
		data:      data,
		withName: true,
	}
}

func (c *Cache) setState(cachePath string, data []byte, withName bool) {
	var path string

	if withName {
		path = cachePath
	} else {
		path = getStatePath(cachePath)
	}

	exists, err := c.fs.Exists(path)
	if err != nil {
		zap.S().Errorw("error checking if state file exists, attempting to create one", "error", err)
	}

	var f afero.File
	if !exists {
		f, err = c.fs.Create(path)
		logger.PanicCheck(err)
	} else {
		f, err = c.fs.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, FILE_MODE)
		logger.PanicCheck(err)
	}
	defer f.Close()

	_, err = f.Write(data)
	logger.PanicCheck(err)

	zap.S().Debugf("cache state updated: %s", path)
}

func (c *Cache) ClearState(cachePath string) {
	c.stateUpdates <- StateUpdate{
		operation: CLEAR,
		cachePath: cachePath,
	}
}

func (c *Cache) clearState(cachePath string) error {
	path := getStatePath(cachePath)
	return c.fs.Remove(path)
}

func (c *Cache) CacheFile(filename string, r io.Reader, perm os.FileMode) error {
	return c.fs.SafeWriteReader(filename, r)
}

func (c *Cache) MakeCacheAll(name string, r io.Reader) error {

	dir, file := filepath.Split(name)

	if dir != "" {
		err := c.fs.MkdirAll(dir, FILE_MODE)
		if err != nil {
			zap.S().Errorw("error attempting to cache data", "object", name, "error", err)
			return err
		}

	}

	rb := bufio.NewReader(r)
	if file != "" && rb.Size() > 0 {
		err := c.CacheFile(name, r, FILE_MODE)
		logger.PanicCheck(err)
	}

	zap.S().Infof("metadata cache file created: %v", name)
	return nil
}

func (c *Cache) CacheCSV(path string, data []byte, options... CSVOption) error {
	o := &CSVOptions{
		nameFromCol: api.ID_FIELD,
	}

	for _, opt := range options {
		opt(o)
	}
	r := bytes.NewReader(data)
	csvReader := gocsv.DefaultCSVReader(r)
	nameIndex := 0

	// Read CSV header, and get index of nameFromCol val
	row, err := csvReader.Read()
	logger.PanicCheck(err)
	for k, v := range row {
		if v == o.nameFromCol {
			nameIndex = k
			break
		}
	}

	header := row
	
	if o.splitRows {
		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			logger.PanicCheck(err)
			rh := [][]string{
				header,
				row,
			}
			csvBytes, err := gocsv.MarshalBytes(rh)
			logger.PanicCheck(err)
			
			cachePath := path + "." + row[nameIndex] + "." + EXT_CSV
			br := bytes.NewReader(csvBytes)
			c.MakeCacheAll(cachePath, br)
		}
	}
	return nil
}

func (c *Cache) DeleteFile(filePath string) error {
	return c.fs.Remove(filePath)
}

func (c *Cache) DeleteAll(cachePath string) error {
	return c.fs.RemoveAll(cachePath)
}

func (c *Cache) Flush() ([]os.FileInfo, error) {
	d, err := c.fs.Open(".")
	if err != nil {
		return nil, err
	}
	defer d.Close()

	list, err := d.Readdir(1)
	if err == io.EOF {
		return nil, err
	}

	return list, nil
}


// CSV Options
type CSVOption func(co *CSVOptions)

type CSVOptions struct {
	header []string
	nameFromCol string
	splitRows bool
}

func SplitCSVRows() CSVOption {
	return func(co *CSVOptions) {
		co.splitRows = true
	}
}

func NameFromColumn(col string) CSVOption {
	return func(co *CSVOptions) {
		co.nameFromCol = col
	}
}
