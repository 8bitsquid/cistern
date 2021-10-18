package auth

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/imdario/mergo"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (
	AUTHENTICATOR_TYPE   = "jwt"
	JWT_EXPIRATION_AFTER = "1h"
	JWT_ALGORITHM        = "RS256"
	JWT_GRANT_TYPE       = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	JWT_FORMAT           = "json"
)

// See the "Salesforce Grants Access Token" section of the Salesforce JWT docs
// https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
type JWTAccessToken struct {
	OAuth2AccessToken `mapstructure:",squash"`
	SfdcSiteURL       string
	SfdcSiteID        string
}

func (at JWTAccessToken) GetAuthHeader() string {
	return at.TokenType + " " + at.AccessToken
}
func (at JWTAccessToken) GetAuthID() string {
	return path.Base(at.ID)
}

// JWTConfig implements Authenticator interface for JWT Auth method
type JWTConfig struct {
	TokenEndpoint  string             `mapstructure:"token_endpoint,omitempty"`
	Algorithm      string             `mapstructure:"algorithm,omitempty"`
	GrantType      string             `mapstructure:"grant_type,omitempty"`
	ExpiresAfter   string             `mapstructure:"expires_after,omitempty"`
	Format         string             `mapstructure:"format,omitempty"`
	PrivateKeyFile string             `mapstructure:"private_key_file,omitempty"`
	PrivateKey     string             `mapstructure:"private_key,omitempty"`
	Claims         jwt.StandardClaims `mapstructure:"claims,omitempty"`

	token         *jwt.Token
	tokenEndpoint *url.URL
}

func NewJWT(clientURL string, config JWTConfig) (*JWTConfig, error) {
	var token *jwt.Token

	jwtConfig := &JWTConfig{
		TokenEndpoint: TOKEN_ENDPOINT,
		Algorithm:     JWT_ALGORITHM,
		ExpiresAfter:  JWT_EXPIRATION_AFTER,
		GrantType:     JWT_GRANT_TYPE,
		Format:        JWT_FORMAT,
	}

	if err := mergo.Merge(jwtConfig, config, mergo.WithOverride); err != nil {
		return nil, err
	}

	// prep claims and attache to token
	tokenEnpoint, err := tools.URLBuilder(clientURL, jwtConfig.TokenEndpoint)
	logger.PanicCheck(err)
	jwtConfig.tokenEndpoint = tokenEnpoint

	expiresAt, err := time.ParseDuration(jwtConfig.ExpiresAfter)
	logger.PanicCheck(err)
	jwtConfig.Claims.ExpiresAt = int64(expiresAt)

	token = jwt.New(jwt.GetSigningMethod(jwtConfig.Algorithm))
	token.Claims = &jwtConfig.Claims

	jwtConfig.token = token
	return jwtConfig, nil
}

func (j *JWTConfig) Authenticate() (client.AccessToken, error) {

	assertion, err := GetAssertion(j)
	if err != nil {
		return nil, err
	}

	data := url.Values{}
	data.Set("grant_type", j.GrantType)
	data.Set("assertion", assertion)

	resp, err := http.PostForm(j.tokenEndpoint.String(), data)
	logger.PanicCheck(err)

	bodyBytes, err := tools.HTTPGetResponseBody(resp)
	logger.PanicCheck(err)

	zap.S().Debugf("Body: ", string(bodyBytes))

	accessToken := JWTAccessToken{}
	err = json.Unmarshal(bodyBytes, &accessToken)
	logger.PanicCheck(err)

	zap.S().Debugw("Access Token", "token", accessToken)

	return accessToken, nil
}

func GetAssertion(j *JWTConfig) (string, error) {
	keyBytes, err := privateKeyBytes(j)
	if err != nil {
		return "", err
	}

	signingKey, err := jwt.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		return "", err
	}

	signedString, err := j.token.SignedString(signingKey)
	if err != nil {
		return "", err
	}

	return signedString, nil
}

func privateKeyBytes(j *JWTConfig) ([]byte, error) {
	if j.PrivateKey != "" {
		return []byte(j.PrivateKey), nil
	}
	return tools.BytesFromFile(j.PrivateKeyFile)
}
