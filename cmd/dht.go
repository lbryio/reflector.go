package cmd

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/reflector.go/dht"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "dht",
		Short: "Run interactive dht node",
		Run:   dhtCmd,
	}
	RootCmd.AddCommand(cmd)
}

func dhtCmd(cmd *cobra.Command, args []string) {
	type rtData struct {
		BlobHashes []string `json:"blob_hashes"`
		Buckets    map[string][]struct {
			Address string   `json:"address"`
			Blobs   []string `json:"blobs"`
			NodeID  string   `json:"node_id"`
		} `json:"buckets"`
		Contacts []string `json:"contacts"`
		NodeID   string   `json:"node_id"`
	}

	bytes, err := ioutil.ReadAll(os.Stdin)
	checkErr(err)

	var data rtData
	err = json.Unmarshal(bytes, &data)
	checkErr(err)

	spew.Dump(data)

	d, err := dht.New(nil)
	checkErr(err)
	err = d.Start()
	checkErr(err)

	for _, nodes := range data.Buckets {
		for _, node := range nodes {
			err = d.Ping(node.Address + ":4444")
			if err != nil {
				log.Errorf("no response from %s", node.Address)
			}
		}
	}

	d.Shutdown()

	return

	rand.Seed(time.Now().UnixNano())

	//d, err := dht.New(&dht.Config{
	//	Address:    "127.0.0.1:21216",
	//	SeedNodes:  []string{"127.0.0.1:21215"},
	//	PrintState: 30 * time.Second,
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//go d.Start()
	//d.WaitUntilJoined()

	nodes := 10
	dhts := dht.MakeTestDHT(nodes)
	defer func() {
		for _, d := range dhts {
			go d.Shutdown()
		}
		time.Sleep(1 * time.Second)
	}()

	wg := &sync.WaitGroup{}
	numIDs := nodes / 2
	ids := make([]dht.Bitmap, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = dht.RandomBitmapP()
	}
	for i := 0; i < numIDs; i++ {
		go func(i int) {
			r := rand.Intn(nodes)
			wg.Add(1)
			defer wg.Done()
			dhts[r].Announce(ids[i])
		}(i)
	}
	wg.Wait()

	go func() {
		for {
			dhts[1].PrintState()
			time.Sleep(10 * time.Second)
		}
	}()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan

	spew.Dump(dhts[1].Announce(dht.BitmapFromHexP("21b2e2d2996b4bef3ff41176668a0577f86aba7f1ea2996edd18f9c42430802c8085311145c5f0c44a7f352e2ba8ae59")))

	<-interruptChan

	for _, d := range dhts {
		go d.Shutdown()
	}
	time.Sleep(1 * time.Second)
}
