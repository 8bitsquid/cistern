package auth

import (
	"github.com/mitchellh/mapstructure"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/client"
	"go.uber.org/zap"
)

const (
	TOKEN_ENDPOINT   = "services/oauth2/token"
	AUTH_ENDPOINT    = "services/oauth2/authorize"
	SUCCESS_ENDPOINT = "services/oauth2/success"
)

// TODO: Support other auth methods
type Auth struct {
	// JWT JWTConfig `mapstructure:"jwt"`
	// UserPass UserPassConfig `mapstructure:"user_pass"`
	OAuth2 OAuth2Config `mapstructure:"oauth2"`
}

type StandardAccessToken struct {
	ID          string
	AccessToken string `json:"access_token,omitempty"`
	TokenType   string `json:"token_type,omitempty"`
	InstanceURL string `json:"instance_url,omitempty"`
}

func AttemptConnectAll(url string, authMethods map[string]interface{}) client.AccessToken {

	var accessToken client.AccessToken
	var auth client.Authenticator
	var err error

authLoop:
	for key, val := range authMethods {
		switch key {
		case CONFIG_OAUTH2_KEY:
			var cfg OAuth2Config
			err = mapstructure.Decode(val, &cfg)
			if err != nil {
				zap.S().Warnw("OAuth2 config invalid")
				break
			}
			auth, err = NewOAuth2(url, cfg)
			if err != nil {
				zap.S().Warnw("OAuth2 config invalid")
				break
			}
			zap.S().Info("Attempting OAuth2 Session")
			accessToken, err = auth.Authenticate()
			if err != nil {
				zap.S().Errorw("unable to connect OAuth2 session", "error", err)
				break
			}
			break authLoop

		case "jwt":
			var cfg JWTConfig
			err := mapstructure.Decode(val, &cfg)
			if err != nil {
				zap.S().Warnw("JWT config invalid")
				break
			}
			auth, err = NewJWT(url, cfg)
			if err != nil {
				zap.S().Warnw("JWT config invalid")
				break
			}

			zap.S().Info("Attempting JWT Session")
			accessToken, err = auth.Authenticate()
			if err != nil {
				zap.S().Errorw("unable to connect JWT session", "error", err)
				break
			}
			break authLoop

		case "userpass":
			var cfg UserPassConfig
			err := mapstructure.Decode(val, &cfg)
			if err != nil {
				zap.S().Warnw("UserPass config invalid")
				break
			}
			auth, err = NewUserPass(url, cfg)
			if err != nil {
				zap.S().Warn("UserPass config invalid")
				break
			}

			zap.S().Info("Attempting UserPass Session")
			accessToken, err = auth.Authenticate()
			if err != nil {
				zap.S().Errorw("unable to connect UserPass session", "error", err)
				break
			}
			break authLoop
		}
	}

	return accessToken
}

// // Web Server Flow
// {
// 	"access_token": "00DB0000000TfcR!AQQAQFhoK8vTMg_rKA.esrJ2bCs.OOIjJgl.9Cx6O7KqjZmHMLOyVb.U61BU9tm4xRusf7d3fD1P9oefzqS6i9sJMPWj48IK",
// 	"signature": "d/SxeYBxH0GSVko0HMgcUxuZy0PA2cDDz1u7g7JtDHw=",
// 	"scope": "web openid",
// 	"id_token": "eyJraWQiOiIyMjAiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdF9oYXNoIjoiSVBRNkJOTjlvUnUyazdaYnYwbkZrUSIsInN1YiI6Imh0dHBzOi8vbG9...",
// 	"instance_url": "https://mycompany.my.salesforce.com",
// 	"id": "https://login.salesforce.com/id/00DB0000000TfcRMAS/005B0000005Bk90IAC",
// 	"token_type": "Bearer",
// 	"issued_at": "1558553873237"
// 	}

// // Refresh
// { "id":"https://login.salesforce.com/id/00Dx0000000BV7z/005x00000012Q9P",
// "issued_at":"1278448384422",
// "instance_url":"https://yourInstance.salesforce.com/",
// "signature":"SSSbLO/gBhmmyNUvN18ODBDFYHzakxOMgqYtu+hDPsc=",
// "access_token":"00Dx0000000BV7z!AR8AQP0jITN80ESEsj5EbaZTFG0RNBaT1cyWk7TrqoDjoNIWQ2ME_sTZzBjfmOE6zMHq6y8PIW4eWze9JksNEkWUl.Cju7m4",
// "token_type":"Bearer",
// "scope":"id api refresh_token"}

// // JWT

// {
// "access_token":"00Dxx0000001gPL!AR8AQJXg5oj8jXSgxJfA0lBog.39AsX.LVpxezPwuX5VAIrrbbHMuol7GQxnMeYMN7cj8EoWr78nt1u44zU31IbYNNJguseu",
// "scope":"web openid api id",
// "instance_url":"https://yourInstance.salesforce.com",
// "id":"https://yourInstance.salesforce.com/id/00Dxx0000001gPLEAY/005xx000001SwiUAAS",
// "token_type":"Bearer"}

// // User Pass
// {"id":"https://login.salesforce.com/id/00Dx0000000BV7z/005x00000012Q9P",
// "issued_at":"1278448832702",
// "instance_url":"https://yourInstance.salesforce.com/",
// "signature":"0CmxinZir53Yex7nE0TD+zMpvIWYGb/bdJh6XfOH6EQ=",
// "access_token":"00Dx0000000BV7z!AR8AQAxo9UfVkh8AlV0Gomt9Czx9LjHnSSpwBMmbRcgKFmxOtvxjTrKW19ye6PE3Ds1eQz3z8jr3W7_VbWmEu4Q8TVGSTHxs",
// "token_type":"Bearer"}
