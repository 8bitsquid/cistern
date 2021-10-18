package client

import "net/http"

type Client interface {
	GetUser() string
	DoClientRequest(*http.Request) (*http.Response, error)
}
