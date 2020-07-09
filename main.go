package main

import (
	"math/rand"
	"time"

	"github.com/lbryio/reflector.go/cmd"

	"github.com/google/gops/agent"
	log "github.com/sirupsen/logrus"
)

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	rand.Seed(time.Now().UnixNano())
	cmd.Execute()
}
