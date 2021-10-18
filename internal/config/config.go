package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
)

const (
	CONFIG_DIR       = ".heb-digital"
	CONFIG_FILE_NAME = "salesforce-backups"
	CONFIG_FILE_EXT  = "yml"
)

var (
	HomeDir   string
	ConfigDir string
)

type Config interface {
	GetConfig(string) interface{}
}

type ConfigFile struct {
	Dir      string
	Filename string
	Ext      string
}

func init() {
	HomeDir = tools.GetHomeDir()
	ConfigDir = filepath.Join(HomeDir, CONFIG_DIR)
}

func NewConfig(path string) (ConfigFile, error) {
	dir, file := filepath.Split(path)
	if file == "" {
		return ConfigFile{}, errors.Unwrap(fmt.Errorf("Invalid file path: %v", path))
	}

	fparts := strings.Split(file, ".")

	// Enforce '.yml' or '.yaml' file extension
	if len(fparts) < 2 || (fparts[1] != "yml" && fparts[1] != "yaml") {
		return ConfigFile{}, errors.Unwrap(fmt.Errorf("Config file must have '.yml' extension: %v", file))
	}

	return ConfigFile{
		Dir:      dir,
		Filename: fparts[0],
		Ext:      fparts[1],
	}, nil
}

func (cf *ConfigFile) AbsolutePath() string {
	fileParts := []string{cf.Filename, cf.Ext}
	f := strings.Join(fileParts, ".")

	return filepath.Join(cf.Dir, f)
}

// TODO: Not Safe - need to use reflection or write proper mapstructure decoder
func ToURLValues(config interface{}) (url.Values, error) {

	var dataMap map[string]string
	mapstructure.Decode(config, &dataMap)

	vals := url.Values{}
	for k, v := range dataMap {
		vals.Set(k, v)
	}

	return vals, nil
}
