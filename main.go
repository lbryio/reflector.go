package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"math/rand"
	"strconv"
	"time"

	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	confFile := flag.String("conf", "config.json", "Config file")
	flag.Parse()

	conf := loadConfig(*confFile)

	peerAddress := "localhost:" + strconv.Itoa(peer.DefaultPort)
	server := peer.NewServer(store.NewS3BlobStore(conf.AwsID, conf.AwsSecret, conf.BucketRegion, conf.BucketName))
	log.Fatal(server.ListenAndServe(peerAddress))
	return

	//
	//address := "52.14.109.125:" + strconv.Itoa(port)
	//reflectorAddress := "localhost:" + strconv.Itoa(reflector.DefaultPort)
	//server := reflector.NewServer(store.NewS3BlobStore(conf.awsID, conf.awsSecret, conf.bucketRegion, conf.bucketName))
	//log.Fatal(server.ListenAndServe(reflectorAddress))

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

type config struct {
	AwsID        string `json:"aws_id"`
	AwsSecret    string `json:"aws_secret"`
	BucketRegion string `json:"bucket_region"`
	BucketName   string `json:"bucket_name"`
}

func loadConfig(path string) config {
	raw, err := ioutil.ReadFile(path)
	checkErr(err)

	var c config
	err = json.Unmarshal(raw, &c)
	checkErr(err)

	return c
}
