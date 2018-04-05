package cmd

import (
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/reflector.go/dht"

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
