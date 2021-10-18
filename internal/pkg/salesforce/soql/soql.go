package soql

import (
	"strings"
	"time"

	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/salesforce/api"
)

type SoqlOption func(o *SoqlOptions) error

type SoqlOptions struct {
	Fields string
	From   string
	Where  []string
}

const (
	SOQL_FIELD_LAST_MODIFIED = "LastModifiedDate"
	TIME_FORMAT              = time.RFC1123Z
)

func SelectFrom(sobj api.SObject, options ...SoqlOption) string {
	o := &SoqlOptions{
		From:  sobj.Name,
		Where: make([]string, 0),
	}

	for _, opt := range options {
		opt(o)
	}

	if o.Fields == "" {
		o.Fields = getAllFields(sobj)
	}

	s := []string{
		"SELECT",
		o.Fields,
		"FROM",
		sobj.Name,
	}

	if len(o.Where) > 0 {
		where := strings.Join(o.Where, ",")
		s = append(s, where)
	}

	return strings.Join(s, " ")
}

func WithFields(fields ...string) SoqlOption {
	return func(o *SoqlOptions) error {
		if len(fields) > 0 {
			o.Fields = strings.Join(fields, ",")
		}
		return nil
	}
}

func getAllFields(sobj api.SObject) string {

	fields := make([]string, 0)

	for _, field := range sobj.Fields {
		fields = append(fields, field.Name)
	}

	return strings.Join(fields, ",")
}

func WhereLastModifiedAfter(t time.Time) SoqlOption {
	return func(o *SoqlOptions) error {
		if !t.IsZero() {
			lm := SOQL_FIELD_LAST_MODIFIED + " >= " + t.Format(TIME_FORMAT)
			o.Where = append(o.Where, lm)
		}
		return nil
	}
}
