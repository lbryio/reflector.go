package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/reflector"
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
	log.SetLevel(log.DebugLevel)

	confFile := flag.String("conf", "config.json", "Config file")
	flag.Parse()

	conf := loadConfig(*confFile)

	db := new(db.SQL)
	err := db.Connect(conf.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(conf.AwsID, conf.AwsSecret, conf.BucketRegion, conf.BucketName)

	combo := store.NewDBBackedS3Store(s3, db)

	serverType := ""
	if len(os.Args) > 1 {
		serverType = os.Args[1]
	}

	switch serverType {
	case "reflector":
		reflectorAddress := "localhost:" + strconv.Itoa(reflector.DefaultPort)
		server := reflector.NewServer(combo)
		log.Fatal(server.ListenAndServe(reflectorAddress))
	case "peer":
		peerAddress := "localhost:" + strconv.Itoa(peer.DefaultPort)
		server := peer.NewServer(combo)
		log.Fatal(server.ListenAndServe(peerAddress))
	default:
		log.Fatal("invalid server type")
	}

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
	DBConn       string `json:"db_conn"`
}

func loadConfig(path string) config {
	raw, err := ioutil.ReadFile(path)
	checkErr(err)

	var c config
	err = json.Unmarshal(raw, &c)
	checkErr(err)

	return c
}
