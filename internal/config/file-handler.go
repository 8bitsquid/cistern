package config

import (
	"bytes"
	"embed"
	"os"
	"text/template"

	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
)

// Uses Go `embed` package to embed templates in binary
//go:embed templates/*
var templatesFS embed.FS

//TODO: Implement config override
func Load() error {

	configFile := ConfigFile{
		Dir:      ConfigDir,
		Filename: CONFIG_FILE_NAME,
		Ext:      CONFIG_FILE_EXT,
	}

	return LoadFile(configFile)
}

func LoadFile(configFile ConfigFile) error {
	// pre-set viper config values
	viper.AddConfigPath(configFile.Dir)
	viper.SetConfigName(configFile.Filename)
	viper.SetConfigType(configFile.Ext)

	// Make sure config dir exists
	dirExists, err := tools.FilePathExists(configFile.Dir)
	logger.PanicCheck(err)
	// Create dir if doesn't exist
	if !dirExists {
		err := os.Mkdir(configFile.Dir, 0777)
		logger.PanicCheck(err)
	}
	// Check config file exists
	fileExists, err := tools.FilePathExists(configFile.AbsolutePath())
	logger.PanicCheck(err)
	// Create if config file doesn't exist
	if !fileExists {
		err := CreateConfig(configFile)
		logger.PanicCheck(err)
	}

	// Read in config
	err = viper.ReadInConfig()
	if err != nil {
		return err
	}

	// If only one host is defined, set it as default
	hosts := viper.GetStringMap("hosts")
	if len(hosts) == 1 {
		for key := range hosts {
			viper.Set("hosts.default", key)
		}
	}

	return nil
}

// Create a new config file and read values into viper
func CreateConfig(config ConfigFile) error {

	//TODO: Possibly add support for different config types?
	tpl := template.Must(template.ParseFS(templatesFS, "templates/yaml-config.tmpl"))

	settings := viper.GetViper().AllSettings()

	var tplBuffer bytes.Buffer
	err := tpl.Execute(&tplBuffer, settings)
	if err != nil {
		return err
	}

	fullPath := config.AbsolutePath()
	e := viper.WriteConfigAs(fullPath)
	if e != nil {
		return e
	}

	return nil
}
