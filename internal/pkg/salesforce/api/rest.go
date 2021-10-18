package api

import (
	"encoding/json"
	"net/http"
	"time"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	REST_ENDPOINT = "/sobjects"

	HEADER_MODIFIED_SINCE   = "If-Modified-Since"
	HEADER_UNMODIFIED_SINCE = "If-Unmodified-Since"

	TIME_FORMAT = time.RFC1123
)

func doRestRequest(req *http.Request, c client.Client) (*http.Response, error) {
	endPoint, err := tools.URLBuilder(REST_ENDPOINT, req.URL.String())
	logger.PanicCheck(err)

	req.URL = endPoint

	resp, err := doAPIRequest(req, c)
	logger.PanicCheck(err)

	return resp, nil
}

// func ModifiedSince(t time.Time) APIOption {
// 	return func(req *http.Request) error {
// 		req.Header.Set(HEADER_MODIFIED_SINCE, t.Format(TIME_FORMAT))
// 		return nil
// 	}
// }

// func UnmodifiedSince(t time.Time) APIOption {
// 	return func(req *http.Request) error {
// 		req.Header.Set(HEADER_UNMODIFIED_SINCE, t.Format(TIME_FORMAT))
// 		return nil
// 	}
// }

type DescribeGlobalResults struct {
	Encoding     string
	MaxBatchSize int
	SObjects     []SObject
}

func DescribeGlobal(options... APIOption) (*DescribeGlobalResults, error) {
	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	req, err := http.NewRequest(http.MethodGet, "", nil)
	logger.PanicCheck(err)

	resp, err := doRestRequest(req, o.client)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	if err != nil {
		return nil, err
	}

	// buf := new(bytes.Buffer)
	// io.Copy(buf, resp.Body)
	// resp.Body.Close()

	results := &DescribeGlobalResults{}
	err = json.Unmarshal(bodyBytes, results)
	if err != nil {
		zap.S().Errorw("error unmarshalling data", string(bodyBytes))
		return nil, err
	}

	return results, nil
}

const (
	END_POINT_DESCRIBE = "describe"
)

func Describe(id string, options... APIOption) (SObject, error) {
	o := &APIOptions{}

	for _, opt := range options {
		opt.applyAPI(o)
	}

	endPoint, err := tools.URLBuilder(id, END_POINT_DESCRIBE)
	if err != nil {
		return SObject{}, err
	}

	req, err := http.NewRequest(http.MethodGet, endPoint.String(), nil)
	logger.PanicCheck(err)

	resp, err := doRestRequest(req, o.client)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	if err != nil {
		return SObject{}, err
	}

	results := SObject{}
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		zap.S().Errorw("error unmarshalling Describe result", "sobject", string(bodyBytes), "error", err)
	}

	return results, err
}
