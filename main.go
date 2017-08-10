package main

import (
	"flag"
	"log"
	"math/rand"
	"time"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	var err error
	rand.Seed(time.Now().UnixNano())

	address := "localhost:5566"

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

	blob = make([]byte, 2*1024*1024)
	_, err = rand.Read(blob)
	checkErr(err)
	err = client.SendBlob(blob)
	checkErr(err)

	blob = make([]byte, 2*1024*1024)
	_, err = rand.Read(blob)
	checkErr(err)
	err = client.SendBlob(blob)
	checkErr(err)
}
