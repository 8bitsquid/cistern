// Code generated by go-enum DO NOT EDIT.

package api

import (
	"fmt"
)

const (
	// ColumnDelimiterBACKQUOTE is a ColumnDelimiter of type BACKQUOTE.
	ColumnDelimiterBACKQUOTE ColumnDelimiter = iota
	// ColumnDelimiterCARET is a ColumnDelimiter of type CARET.
	ColumnDelimiterCARET
	// ColumnDelimiterCOMMA is a ColumnDelimiter of type COMMA.
	ColumnDelimiterCOMMA
	// ColumnDelimiterPIPE is a ColumnDelimiter of type PIPE.
	ColumnDelimiterPIPE
	// ColumnDelimiterSEMICOLON is a ColumnDelimiter of type SEMICOLON.
	ColumnDelimiterSEMICOLON
	// ColumnDelimiterTAB is a ColumnDelimiter of type TAB.
	ColumnDelimiterTAB
)

const _ColumnDelimiterName = "BACKQUOTECARETCOMMAPIPESEMICOLONTAB"

var _ColumnDelimiterMap = map[ColumnDelimiter]string{
	0: _ColumnDelimiterName[0:9],
	1: _ColumnDelimiterName[9:14],
	2: _ColumnDelimiterName[14:19],
	3: _ColumnDelimiterName[19:23],
	4: _ColumnDelimiterName[23:32],
	5: _ColumnDelimiterName[32:35],
}

// String implements the Stringer interface.
func (x ColumnDelimiter) String() string {
	if str, ok := _ColumnDelimiterMap[x]; ok {
		return str
	}
	return fmt.Sprintf("ColumnDelimiter(%d)", x)
}

var _ColumnDelimiterValue = map[string]ColumnDelimiter{
	_ColumnDelimiterName[0:9]:   0,
	_ColumnDelimiterName[9:14]:  1,
	_ColumnDelimiterName[14:19]: 2,
	_ColumnDelimiterName[19:23]: 3,
	_ColumnDelimiterName[23:32]: 4,
	_ColumnDelimiterName[32:35]: 5,
}

// ParseColumnDelimiter attempts to convert a string to a ColumnDelimiter
func ParseColumnDelimiter(name string) (ColumnDelimiter, error) {
	if x, ok := _ColumnDelimiterValue[name]; ok {
		return x, nil
	}
	return ColumnDelimiter(0), fmt.Errorf("%s is not a valid ColumnDelimiter", name)
}
