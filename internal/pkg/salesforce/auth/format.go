package auth

// There is no enum type in Go. Current enum methods are odd, but I wanted to try it out
// Proposal is still in discussion - https://github.com/golang/go/issues/19814
type FormatType int

const (
	JSON FormatType = iota
	XML
	URL_ENCODED
)

func (ft FormatType) String() string {
	return []string{"json", "xml", "urlencoded"}[ft]
}

