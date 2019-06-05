package main

import (
	"math/rand"
	"time"

	"github.com/lbryio/reflector.go/cmd"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	cmd.Execute()
}
