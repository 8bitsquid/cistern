package salesforce

import (
	"github.com/sigmavirus24/salesforceid"
	"go.uber.org/zap"
)

func PKChunkRange(id string, offset uint64) (string, string, error) {
	firstID, err := salesforceid.New(id)
	if err != nil {
		zap.S().Errorf("invalid Salesforce ID: %s", id)
		return "", "", err
	}

	lastID, err := firstID.Add(offset)
	if err != nil {
		zap.S().Errorf("invalid offset for PKChunk range: %v", offset)
		return "", "", err
	}

	return firstID.String(), lastID.String(), nil
}