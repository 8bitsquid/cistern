package tools

import (
	"fmt"
)

func StringSliceContaines(s []string, c string) bool {
	for _, v := range s {
		if c == v {
			return true
		}
	}
	return false
}

func SliceChunkFunc(chunkSize int, s []interface{}, f func(...interface{}) interface{}){
	if len(s) % chunkSize != 0 {
		err := fmt.Sprintf("SliceChunkFunc out of bounds chunk size - chunkSize: %d, slice length: %d", chunkSize,len(s))
		panic(err)
	}

	for i := 0; i < len(s); i += chunkSize {
		end := i+chunkSize
		chunk := s[i:end]
		f(chunk...)
	}
}

func StringSlicePopChunk(chunkSize int, s []string) (chunk []string, leftOver []string) {
	chunk = make([]string, 0, (len(s) + chunkSize - 1) / chunkSize)
	return append(chunk, s[:chunkSize]...), s[chunkSize:]
}



const (
	SHUTTLE_LEFT  = -1
	SHUTTLE_RIGHT = 1
)

// StringSliceWeave will interlace `this` inbetween each item in `that` slice
// The position of `this` on the weft shuttle direction
//
// Examples:
// this = z , that = [I, I, I], weft = SHUTTLE_LEFT
// result: [z, I, z, I, z, I]
//
// this = z , that = [I,I,I], weft = SHUTTLE_RIGHT
// result: [I, z, I, z, I, z]
//
// .... and yes, I made a function into a loom metaphor... sorry...


func StringSliceWeave(this string, into []string, weft int) []string {
	i := 0
	l := len(into)*2

	for i < l {
		into = append(into[:i+1], into[i:]...)
		into[i + weft] = this
		
		i += 2
	}

	return into
}