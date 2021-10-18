package auth

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/imdario/mergo"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/browser"
	"go.uber.org/zap"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
)

const (
	OAUTH2_RESPONSE_TYPE = "token"

	CONFIG_OAUTH2_KEY = "oauth2"
)

type OAuth2Config struct {
	ClientID     string   `mapstructure:"client_id,omitempty"`
	RedirectURI  string   `mapstructure:"redirect_uri,omitempty"`
	ReseponsType string   `mapstructure:"resepons_type,omitempty"`
	Scope        []string `mapstructure:"scope,omitempty"`
	State        string   `mapstructure:"state,omitempty"`
	Display      string   `mapstructure:"display,omitempty"`
	LoginHint    string   `mapstructure:"login_hint,omitempty"`
	Nonce        string   `mapstructure:"nonce,omitempty"`
	Prompt       string   `mapstructure:"prompt,omitempty"`

	clientURL string
}

func NewOAuth2(clientURL string, oa2 OAuth2Config) (*OAuth2Config, error) {
	redirectURI, err := tools.URLBuilder(clientURL, SUCCESS_ENDPOINT)
	logger.PanicCheck(err)

	logger.PanicCheck(err)
	oauth2Config := &OAuth2Config{
		RedirectURI:  redirectURI.String(),
		ReseponsType: OAUTH2_RESPONSE_TYPE,
		clientURL:    clientURL,
	}

	if err := mergo.Merge(oauth2Config, oa2, mergo.WithOverride); err != nil {
		return nil, err
	}

	// authEndpoint, err := tools.URLBuilder(clientURL, AUTH_ENDPOINT)
	// logger.PanicCheck(err)

	// tokenEndpoint, err := tools.URLBuilder(clientURL, TOKEN_ENDPOINT)
	// logger.PanicCheck(err)

	return oauth2Config, nil
}

func (o *OAuth2Config) Authenticate() (client.AccessToken, error) {
	//https://heb--willdev.my.salesforce.com/services/oauth2/authorize?response_type=token&client_id=3MVG9pHRjzOBdkd.PTG4KZTEYvfydzegVqi9f0IfYQcJK8GzfEGwjNMEKM4B5snYul.jSFDLlsf_5O327a0Eh&redirect_uri=https://heb--willdev.my.salesforce.com/services/oauth2/success

	data := url.Values{
		"response_type": {o.ReseponsType},
		"client_id":     {o.ClientID},
		"redirect_uri":  {o.RedirectURI},
	}
	authURL, err := tools.URLBuilder(o.clientURL, AUTH_ENDPOINT)
	logger.PanicCheck(err)

	authURL.RawQuery = data.Encode()
	zap.S().Debugf("Auth URL: %v", authURL.String())

	err = browser.OpenURL(authURL.String())
	logger.PanicCheck(err)

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Copy/Paste redirected URL: ")
	u, err := reader.ReadString('\n')
	logger.PanicCheck(err)

	uParts := strings.SplitAfter(u, "success#")
	if len(uParts) > 1 {
		query := strings.TrimSuffix(uParts[1], "\n")
		vals, err := url.ParseQuery(query)
		logger.PanicCheck(err)

		// TODO: cheap work around to decode into struct with mapstructure - I hate this - make it better
		valMap := map[string]string{
			"id":           vals.Get("id"),
			"access_token": vals.Get("access_token"),
			"instance_url": vals.Get("instance_url"),
			"issued_at":    vals.Get("issued_at"),
			"signature":    vals.Get("signature"),
			"scope":        vals.Get("scope"),
			"type_type":    vals.Get("token_type"),
		}

		issuedAt, err := strconv.ParseInt(vals.Get("issued_at"), 10, 64)
		logger.PanicCheck(err)

		accessToken := &OAuth2AccessToken{
			StandardAccessToken: StandardAccessToken{
				ID:          vals.Get("id"),
				AccessToken: vals.Get("access_token"),
				InstanceURL: vals.Get("instance_url"),
				TokenType:   vals.Get("token_type"),
			},
			IssuedAt:  time.Unix(issuedAt, 0),
			Signature: vals.Get("signature"),
			Scope:     strings.Split(vals.Get("scope"), " "),
		}
		mapstructure.Decode(valMap, accessToken)

		return accessToken, nil
	}

	return nil, fmt.Errorf("failed to authenticate OAuth2 session: %v", u)
}

type OAuth2AccessToken struct {
	StandardAccessToken `mapstructure:",squash"`
	RefreshToken        string
	State               string
	IssuedAt            time.Time
	Scope               []string
	Signature           string
}

func (at *OAuth2AccessToken) GetAuthHeader() string {
	return at.TokenType + " " + at.AccessToken
}

func (at *OAuth2AccessToken) GetAuthID() string {
	return path.Base(at.ID)
}
