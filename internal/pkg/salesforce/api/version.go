package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/logger"
)

// Version is a custom type to correspond to Salesforce API version formatting in requests and URL paths
// A Version split into parts, using "." as a delimiter. Parts are validated and converted to thier `int` value.
//
// When using a Version type in `string` operations, the version is prefixed by "v" to correspond to Salesforce API versioned URL Paths
// For example, if we have a Version :
//
//
type Version struct {
	parts []int
}

func NewVersion(v float32) Version {
	var b strings.Builder
	fmt.Fprintf(&b, "%.1f", v)

	s := b.String()

	parts, _ := stringToParts(s)
	//logger.PanicCheck(err)

	return Version{parts}
}

func stringToParts(s string) ([]int, error) {
	s = strings.TrimPrefix(s, "v")
	sParts := strings.Split(s, ".")
	parts := make([]int, len(sParts))

	for i, p := range sParts {
		parts[i], _ = strconv.Atoi(p)

	}

	return parts, nil
}

func versionToString(parts []int) (string, error) {
	if len(parts) < 1 {
		return "", errors.New("no value for Version")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "v%d", parts[0])
	for _, p := range parts[1:] {
		_, err := fmt.Fprintf(&b, ".%d", p)
		if err != nil {
			return "", nil
		}
	}

	return b.String(), nil
}

func (v Version) String() string {
	ver, err := versionToString(v.parts)
	logger.PanicCheck(err)

	return ver
}

func (v *Version) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	logger.PanicCheck(err)

	s = strings.TrimPrefix(s, "v")
	sParts := strings.Split(s, ".")
	parts := make([]int, len(sParts))

	for i, p := range sParts {
		parts[i], err = strconv.Atoi(p)
		if err != nil {
			return err
		}
	}

	v.parts = parts
	return nil
}

func (v Version) MarshalJSON() ([]byte, error) {
	ver, err := versionToString(v.parts)
	if err != nil {
		return nil, err
	}

	return []byte(ver), nil
}
