package meta

import (
	"fmt"
	"strconv"
	"time"
)

var (
	name       = "prism-bin"
	version    = "unknown"
	commit     = "unknown"
	commitLong = "unknown"
	branch     = "unknown"
	Time       = "unknown"
	BuildTime  time.Time
)

// Name returns main application name
func Name() string {
	return name
}

// Version returns current application version
func Version() string {
	return version
}

// FullName returns current app version, commit and build time
func FullName() string {
	return fmt.Sprintf(
		`Name: %v
Version: %v
branch: %v
commit: %v
commit long: %v
build date: %v`, Name(), Version(), branch, commit, commitLong, BuildTime.String())
}

func init() {
	if Time != "" {
		t, err := strconv.Atoi(Time)
		if err == nil {
			BuildTime = time.Unix(int64(t), 0).UTC()
		}
	}
}

func VersionString() string {
	var buildTime string
	if BuildTime.IsZero() {
		buildTime = "<now>"
	} else {
		buildTime = BuildTime.Format(time.RFC3339)
	}

	return fmt.Sprintf("version %s, built %s", version, buildTime)
}
