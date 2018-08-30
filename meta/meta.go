package meta

import (
	"strconv"
	"time"
)

var Version = ""
var Time = ""
var BuildTime time.Time

func init() {
	if Time != "" {
		t, err := strconv.Atoi(Time)
		if err != nil {
			return
		}
		BuildTime = time.Unix(int64(t), 0).UTC()
	}
}
