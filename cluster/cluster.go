package cluster

import (
	"io/ioutil"
	baselog "log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/lbry.go/crypto"
	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/reflector.go/cluster"

	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
)

type Cluster struct {
	s       *serf.Serf
	eventCh <-chan serf.Event
}

func New() {
	c := &Cluster{}
	var err error

	nodeName := crypto.RandString(12)
	clusterAddr := "127.0.0.1:" + strconv.Itoa(clusterPort)
	if args[0] == clusterStart {
		c.s, c.eventCh, err = cluster.Connect(nodeName, clusterAddr, clusterPort)
	} else {
		c.s, c.eventCh, err = cluster.Connect(nodeName, clusterAddr, clusterPort+1+int(crypto.RandInt64(1000)))
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
						//spew.Dump(c.Members())
						alive := getAliveMembers(c.Members())
						log.Printf("%s: my hash range is now %d of %d\n", nodeName, getHashRangeStart(nodeName, alive), len(alive))
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

func Connect(nodeName, addr string, port int) (*serf.Serf, <-chan serf.Event, error) {
	conf := serf.DefaultConfig()
	conf.MemberlistConfig.BindPort = port
	conf.MemberlistConfig.AdvertisePort = port
	conf.NodeName = nodeName

	nullLogger := baselog.New(ioutil.Discard, "", 0)
	conf.Logger = nullLogger

	eventCh := make(chan serf.Event)
	conf.EventCh = eventCh

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, nil, errors.Prefix("couldn't create cluster", err)
	}

	_, err = cluster.Join([]string{addr}, true)
	if err != nil {
		log.Warnf("couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, eventCh, nil
}
