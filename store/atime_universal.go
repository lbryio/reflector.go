// +build !linux

package store

import (
	"os"
	"time"
)

func atime(fi os.FileInfo) time.Time {
	return fi.ModTime()
}
