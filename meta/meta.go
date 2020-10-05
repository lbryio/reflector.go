package meta

import (
	"fmt"
	"strconv"
	"time"
)

var Version = ""
var Time = ""
var BuildTime time.Time

func init() {
	if Time != "" {
		t, err := strconv.Atoi(Time)
		if err == nil {
			BuildTime = time.Unix(int64(t), 0).UTC()
		}
	}
}

func VersionString() string {
	version := Version
	if version == "" {
		version = "<unset>"
	}

	var buildTime string
	if BuildTime.IsZero() {
		buildTime = "<now>"
	} else {
		buildTime = BuildTime.Format(time.RFC3339)
	}

	return fmt.Sprintf("version %s, built %s", version, buildTime)
}
