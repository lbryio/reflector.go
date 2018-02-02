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

	//
	//var err error
	//client := reflector.Client{}
	//
	//log.Println("Connecting to " + reflectorAddress)
	//err = client.Connect(reflectorAddress)
	//checkErr(err)
	//
	//log.Println("Connected")
	//
	//defer func() {
	//	log.Println("Closing connection")
	//	client.Close()
	//}()
	//
	//blob := make([]byte, 2*1024*1024)
	//_, err = rand.Read(blob)
	//checkErr(err)
	//err = client.SendBlob(blob)
	//checkErr(err)
}

func loadConfig(path string) cmd.Config {
	raw, err := ioutil.ReadFile(path)
	checkErr(err)

	var c cmd.Config
	err = json.Unmarshal(raw, &c)
	checkErr(err)

	return c
}
