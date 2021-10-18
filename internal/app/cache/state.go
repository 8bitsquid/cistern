package cache

import (
	"path/filepath"
)

// state data structures and helper functions
type StateOperation int
const (
	STATE_FILE = `.state`

	UPDATE StateOperation = iota
	CLEAR
)

type StateUpdate struct {
	operation StateOperation
	cachePath string
	data []byte
	withName bool
}

// func stateFromValues(stateVals... interface{}) []byte {
// 	s := ""
// 	for _, v := range stateVals {
// 		s = fmt.Sprintf("%v%v\n", s, v)
// 	}
// 	zap.S().Debugf("State generated from values: %v", s)
// 	return []byte(s)
// }

func getStatePath(cachePath string) string {
	path, file := filepath.Split(cachePath)

	// If no extention, then it's only a cache and not a file or `.state` file
	ext := filepath.Ext(file)
	if (ext == "") {
		path = file
	}

	path = filepath.Join(path, STATE_FILE)
	return path
}




