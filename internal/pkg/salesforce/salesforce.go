package salesforce

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/viper"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/auth"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	DEFAULT_URL         = "https://test.salesforce.com"
	DEFAULT_API_VERSION = "51.0"

	CONFIG_KEY = "salesforce"
)

type SalesforceConfig struct {
	URL  string                 `mapstructure:"url"`
	Auth map[string]interface{} `mapstructure:"auth"`
}

type Salesforce struct {
	URL  string                 `mapstructure:"url"`
	Auth map[string]interface{} `mapstructure:"auth"`

	accessToken client.AccessToken
	userID      string
}

func NewSession() (*Salesforce, error) {
	return NewSessionFromConfig(CONFIG_KEY)
}

func NewSessionFromConfig(key string) (*Salesforce, error) {
	sf := &Salesforce{}
	err := viper.UnmarshalKey(key, sf)
	logger.PanicCheck(err)

	accessToken := auth.AttemptConnectAll(sf.URL, sf.Auth)
	sf.accessToken = accessToken

	userID := accessToken.GetAuthID()
	sf.userID = userID

	return sf, nil
}

func (s *Salesforce) GetUser() string {
	return s.userID
}

func (s *Salesforce) DoClientRequest(req *http.Request) (resp *http.Response, err error){

	// Recover on Salesforce API error
	// re-panic if error is not a SalesforceError
	defer func ()  {
		if e := recover(); e != nil {
			err = e.(SalesforceError)
		}
	}()

	// inject access token header
	req.Header.Set("Authorization", s.accessToken.GetAuthHeader())

	url, err := tools.URLBuilder(s.URL, req.URL.String())
	logger.PanicCheck(err)

	req.URL = url

	resp, err = http.DefaultClient.Do(req)
	logger.PanicCheck(err)

	if resp.StatusCode >= 400 {
		bodyBytes, err := tools.HTTPGetResponseBody(resp)
		logger.PanicCheck(err)
		
		sfErr := ParseSalesforceError(bodyBytes)
		s.error(SalesforceError{
			sfErr[0],
			resp.Status,
		})
	}

	return resp, nil
}

func (s *Salesforce) error(err SalesforceError) {
	panic(SalesforceError(err))
}

// Salesforce errors
type SalesforceError struct {
	sfError
	HttpStatus string
}

func (se SalesforceError) Error() string {
	return fmt.Sprintf("[Salesforce Error] httpStatus: %v, errorCode: %v, message: %v, fields: %v", se.HttpStatus, se.ErrorCode, se.Message, se.Fields)
}

type sfError struct {
	Fields     []string `json:",omitempty"`
	Message    string   `json:",omitempty"`
	ErrorCode  string   `json:",omitempty"`
}

func ParseSalesforceError(body []byte) []sfError {
	zap.S().Debugw("parsing salesforce error", "body", string(body))
	sfe := make([]sfError, 0)
	err := json.Unmarshal(body, &sfe)
	logger.PanicCheck(err)

	return sfe
}
