package tools

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"go.uber.org/zap"
)

func GetHomeDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Unable to find user's home directory: %+v", err)
	}
	return homeDir
}

// TODO: Move to tools package
// Checks if the given filepath exists on local disk
func FilePathExists(filePath string) (bool, error) {
	zap.S().Infof("Checking if path exists: %v", filePath)
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, nil
}

func BytesFromFile(filePath string) ([]byte, error) {
	if _, err := FilePathExists(filePath); err != nil {
		return nil, err
	}
	return ioutil.ReadFile(filePath)
}

func FileModeFromString(mode string) (os.FileMode, error) {
	m, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		zap.S().Errorf("invalid os.FileMode:, %v", mode)
		logger.PanicCheck(err)
	}

	return os.FileMode(m), nil
}

