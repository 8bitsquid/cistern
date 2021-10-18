package tools

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Month is skipped since it needs to be calculated and can't be a constant duration
const (
	Day = 24 * time.Hour
	Week = 7 * Day
)

var unitMap = map[string]int64{
	"h": int64(time.Hour),
	"d": int64(Day),
	"w": int64(Week),
}

func ParseDuration(dur string) (time.Duration, error) {
	if !(dur[0] >= '0' && dur[0] <= '9') {
		return 0, errors.New("tools/time: invalid duration " + dur)
	}

	// holds the int64 value for Day/Week only. Otherwise it should always be 0
	duration := dur
	var d int64

	// Find leading numbers and unit index 
	for dur != "" {
		num, u, remaining := SplitLeadingDuration(dur)

		// check if unit is Hour/Day/Week - otherwise exit loop
		unit, ok := unitMap[u]
		if !ok {
			break
		}
		dur = remaining

		var err error
		// Convert leading number to int64
		dVal, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, err
		}
		
		d = d + (dVal * unit)
	}

	// if any hour/day/week were defined, convert to time.Duration parseable format
	if d > 0 {
		hours := time.Duration(d).Hours()
		duration = fmt.Sprintf("%vh%s", hours, dur)
	}	

	return time.ParseDuration(duration)
}

func SplitLeadingDuration(s string) (string, string, string) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
	}

	return s[:i], string(s[i]), s[i+1:]
}