//go:generate go-enum -f=$GOFILE
package api

// There is no enum type in Go. Proposal is still in discussion - https://github.com/golang/go/issues/19814
// Enum Types are generated using go-enum - https://github.com/abice/go-enum

/*
ENUM(
	BACKQUOTE,
	CARET,
	COMMA,
	PIPE,
	SEMICOLON,
	TAB,
)
*/
type ColumnDelimiter int