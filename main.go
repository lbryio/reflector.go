package main

import (
	"flag"
	"math/rand"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	port := DefaultPort
	//address := "52.14.109.125:" + strconv.Itoa(port)
	address := "localhost:" + strconv.Itoa(port)

	serve := flag.Bool("server", false, "Run server")
	blobDir := flag.String("blobdir", "", "Where blobs will be saved to")
	flag.Parse()
	if serve != nil && *serve {
		if blobDir == nil || *blobDir == "" {
			log.Fatal("-blobdir required")
		}
		server := NewServer(*blobDir)
		log.Fatal(server.ListenAndServe(address))
		return
	}

	var err error
	client := Client{}

	log.Println("Connecting to " + address)
	err = client.Connect(address)
	checkErr(err)

	log.Println("Connected")

	defer func() {
		log.Println("Closing connection")
		client.Close()
	}()

	blob := make([]byte, 2*1024*1024)
	_, err = rand.Read(blob)
	checkErr(err)
	err = client.SendBlob(blob)
	checkErr(err)
}
