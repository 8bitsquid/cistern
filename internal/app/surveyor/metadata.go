package surveyor

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/Jeffail/tunny"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/soql"
	"go.uber.org/zap"
)



func DiscoverMetadata(s *Surveyor) error {
	zap.S().Info("Getting basic object metadata")
	basicData, err := api.DescribeGlobal(api.WithClient(s.client))
	if err != nil {
		return err
	}

	stop := make(chan struct{})

	s.MetadataWorkers = tunny.NewFunc(s.numMetadataWorkers, func(i interface{}) interface{} {
		req := i.(RecordMetadataRequest)
		RecordMetadata(req)
		return nil
	})

	// TODO debug to clean up
	go func() {
		<-time.After(5 * time.Second)
		stop <-struct{}{}
	}()
	zap.S().Infof("%d objects found", len(basicData.SObjects))
	zap.S().Info("Getting detailed metadata for each object")
	for _, sobj := range basicData.SObjects {
		zap.S().Debugf("Getting full metadata for %s", sobj.Name)
		sobject, err := api.Describe(sobj.Name, api.WithClient(s.client))
		if err != nil {
			zap.S().Errorf("unable to get details for %s", sobj.Name)
			continue
		}

		zap.S().Infof("Recording metadata for %s", sobject.Name)
		request := RecordMetadataRequest{
			sobject: sobject,
			s: s,
		}
		s.MetadataWorkers.Process(request)

		// TODO debug to clean up
		select {
		case <-stop:
			s.MetadataWorkers.Close()
			return nil
		default:
		}
	}

	return nil
}

type RecordMetadataRequest struct {
	sobject api.SObject
	s *Surveyor
}

func RecordMetadata(rmr RecordMetadataRequest) error {
	defer func ()  {
		if e := recover(); e != nil {
			zap.S().Errorw("unable to record metadata", "error", e, "object", rmr.sobject.Name)
		}
	}()

	path := filepath.Join(rmr.sobject.Name, METADATA_FILE_NAME)
	data, err := json.Marshal(rmr.sobject)
	logger.PanicCheck(err)

	err = rmr.s.cache.MakeCacheAll(path, bytes.NewReader(data))
	logger.PanicCheck(err)

	// Only request records for queryable sobjects
	if rmr.sobject.Queryable {
		query := soql.SelectFrom(rmr.sobject, soql.WhereLastModifiedAfter(rmr.s.lastModified))
		rmr.s.recordsRequest <-query
	}

	return nil
}

