package api

// There is no enum type in Go. Current enum methods are odd, but I wanted to try it out
// Proposal is still in discussion - https://github.com/golang/go/issues/19814
type QueryType int

const (
	QUERY QueryType = iota
	QUERY_ALL
)


func (qt QueryType) String() string {
	return []string{"query", "queryAll"}[qt]
}



