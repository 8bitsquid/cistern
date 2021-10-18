package api

import (
	"net/http"
	"net/url"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
)

const (
	API_BASE_PATH = "/services/data"
	API_VERSION   = "v51.0"

	ID_FIELD = "Id"
)

func doAPIRequest(req *http.Request, c client.Client) (resp *http.Response, err error) {
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(APIError)
		}
	}()
	
	u, err := tools.URLBuilder(API_BASE_PATH, API_VERSION, req.URL.String())
	logger.PanicCheck(err)

	req.URL = u
	return c.DoClientRequest(req)
}

type APIError string 

func (ae APIError) Error() string {
	return string(ae)
}

type APIOption interface {
	applyAPI(*APIOptions)
}

type APIOptions struct {
	client         client.Client
	filters        []apiFilter
	urlQueryParams url.Values
	requestBody    APIRequestBody
}

// WithClient is a functional option to assign a client to an api call
func WithClient(c client.Client) APIOption {
	return withClient{c}
}

type withClient struct {
	client client.Client
}

func (wc withClient) applyAPI(o *APIOptions) {
	o.client = wc.client
}

type APIRequestBody struct {
	Operation       string `json:"operation,omitempty"`
	Query           string `json:"query,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	ColumnDelimiter string `json:"column_delimiter,omitempty"`
	LineEnding      string `json:"line_ending,omitempty"`
	State           string `json:",omitempty"`
}

const (
	FILTER_OPERATION_INCLUDE = "include"
	FILTER_OPERATION_EXCLUDE = "exclude"
)

type apiFilter struct {
	operation string
	key       string
	val       string
}

func (af apiFilter) applyAPI(o *APIOptions) {
	o.filters = append(o.filters, af)
}

func CreatedById(userID string) APIOption {
	return apiFilter{FILTER_OPERATION_INCLUDE, "CreatedById", userID}
}

//TODO: make filtering more dynamic
// func Include(key string, val string) APIOption {
// 	return apiFilter{FILTER_OPERATION_INCLUDE, key, val}
// }

// func Exclude(key string, val string) APIOption {
// 	return apiFilter{FILTER_OPERATION_EXCLUDE, key, val}
// }

