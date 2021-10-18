package client

type Authenticator interface {
	Authenticate() (AccessToken, error)
}

type AccessToken interface {
	GetAuthHeader() string
	GetAuthID() string
}