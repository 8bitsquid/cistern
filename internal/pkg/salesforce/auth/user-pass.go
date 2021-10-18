package auth

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/config"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	USER_PASS_GRANTE_TYPE = "password"
	USER_PASS_FORMET      = "json"
)

// https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5
// type userPassAccessToken struct {
// 	OAuth2AcessToken
// }

type UserPassConfig struct {
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	GrantType    string `mapstructure:"grant_type"`
	Format       string `mapstructure:"format"`
}

type UserPass struct {
	config        *UserPassConfig
	tokenEndpoint url.URL
}

func NewUserPass(clientURL string, config UserPassConfig) (*UserPass, error) {
	zap.S().Info("Building new UserPass auth config")
	if config.GrantType == "" {
		config.GrantType = USER_PASS_GRANTE_TYPE
	}

	tokenEndpoint, err := tools.URLBuilder(clientURL, TOKEN_ENDPOINT)
	logger.PanicCheck(err)
	zap.S().Debugf("using token endpoint", tokenEndpoint)

	up := &UserPass{
		config:        &config,
		tokenEndpoint: *tokenEndpoint,
	}

	return up, nil
}

func (u *UserPass) Authenticate() (client.AccessToken, error) {
	zap.S().Info("Attempting UserPass auth flow")
	data, err := config.ToURLValues(u.config)
	logger.PanicCheck(err)

	zap.S().Infof("requesting access token from endpoint", u.tokenEndpoint.String())
	zap.S().Debugf("request data", data.Encode())
	res, err := http.PostForm(u.tokenEndpoint.String(), data)
	// res, err := http.PostForm(u.tokenEndpoint.String(), url.Values{
	// 	"client_id": {u.config.ClientID},
	// 	"client_secret": {u.config.ClientSecret},
	// 	"grant_type": {USER_PASS_GRANTE_TYPE},
	// 	"username": {u.config.Username},
	// 	"password": {u.config.Password},
	// })
	logger.PanicCheck(err)

	zap.S().Debugf("UserPass auth response", res)

	bodyBuff := new(bytes.Buffer)
	io.Copy(bodyBuff, res.Body)
	res.Body.Close()

	zap.S().Debugf("UserPass authenticate body", bodyBuff.Bytes())

	accessToken := make(map[string]interface{})
	err = json.Unmarshal(bodyBuff.Bytes(), &accessToken)
	logger.PanicCheck(err)

	zap.S().Debugf("access token retrieved", accessToken)
	return nil, nil
}
