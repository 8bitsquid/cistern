package tools

import (
	"net/url"
	"path"
	"strings"
)

func URLBuilder(baseURL string, uriParts... string) (*url.URL, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	uri := path.Join(uriParts...)
	u.Path = path.Join(u.Path, uri)
	return u, nil
}

func URLQueryToMap(query string) (map[string]string) {
	query = strings.TrimPrefix(query, "?")
	parts := strings.Split(query, "&")

	queryMap := make(map[string]string, len(parts))
	for _, p := range parts {
		fieldVal := strings.Split(p, "=")
		queryMap[fieldVal[0]] = fieldVal[1]
	}

	return queryMap
}
