package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/lbryio/reflector.go/cmd"

	log "github.com/sirupsen/logrus"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetLevel(log.DebugLevel)
	cmd.GlobalConfig = loadConfig("config.json")
	cmd.Execute()
}

func loadConfig(path string) cmd.Config {
	raw, err := ioutil.ReadFile(path)
	checkErr(err)

	var c cmd.Config
	err = json.Unmarshal(raw, &c)
	checkErr(err)

	return c
}
