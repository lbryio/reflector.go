package cmd

import (
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"

	"github.com/hashicorp/serf/serf"
	"github.com/lbryio/internal-apis/app/crypto"
	"github.com/lbryio/reflector.go/cluster"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	clusterStart = "start"
	clusterJoin  = "join"

	clusterPort = 17946
)

func init() {
	var cmd = &cobra.Command{
		Use:       "cluster [start|join]",
		Short:     "Connect to cluster",
		ValidArgs: []string{clusterStart, clusterJoin},
		Args:      argFuncs(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Run:       clusterCmd,
	}
	RootCmd.AddCommand(cmd)
}

func clusterCmd(cmd *cobra.Command, args []string) {
	var c *serf.Serf
	var eventCh <-chan serf.Event
	var err error

	nodeName := crypto.RandString(12)
	clusterAddr := "127.0.0.1:" + strconv.Itoa(clusterPort)
	if args[0] == clusterStart {
		c, eventCh, err = cluster.Connect(nodeName, clusterAddr, clusterPort)
	} else {
		c, eventCh, err = cluster.Connect(nodeName, clusterAddr, clusterPort+1+rand.Intn(1000))
	}
	if err != nil {
		log.Fatal(err)
	}
	defer c.Leave()

	shutdownCh := make(chan struct{})
	var shutdownWg sync.WaitGroup

	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		for {
			select {
			case event := <-eventCh:
				spew.Dump(event)
				switch event.EventType() {
				case serf.EventMemberJoin, serf.EventMemberFailed, serf.EventMemberLeave:
					memberEvent := event.(serf.MemberEvent)
					if event.EventType() == serf.EventMemberJoin && len(memberEvent.Members) == 1 && memberEvent.Members[0].Name == nodeName {
						// ignore event from my own joining of the cluster
					} else {
						spew.Dump(c.Members())
						log.Printf("my hash range is now %d\n", getHashRangeStart(nodeName, getAliveMembers(c.Members())))
						// figure out my new hash range based on the start and the number of alive members
						// get hashes in that range that need announcing
						// announce them
						// if more than one node is announcing each hash, figure out how to deal with last_announced_at so both nodes dont announce the same thing at the same time
					}
				}
			case <-shutdownCh:
				log.Debugln("shutting down event dumper")
				return
			}
		}
	}()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	log.Debugln("received interrupt")
	close(shutdownCh)
	log.Debugln("waiting for threads to finish")
	shutdownWg.Wait()
	log.Debugln("shutting down main thread")
}

func getHashRangeStart(myName string, members []serf.Member) int {
	names := []string{}
	for _, m := range members {
		names = append(names, m.Name)
	}

	sort.Strings(names)
	i := 1
	for _, n := range names {
		if n == myName {
			return i
		}
		i++
	}
	return -1
}

func getAliveMembers(members []serf.Member) []serf.Member {
	alive := []serf.Member{}
	for _, m := range members {
		if m.Status == serf.StatusAlive {
			alive = append(alive, m)
		}
	}
	return alive
}
