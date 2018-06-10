package cluster

import (
	"io/ioutil"
	baselog "log"
	"sort"

	"github.com/lbryio/lbry.go/crypto"
	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/stopOnce"

	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultClusterPort is the default port used when starting up a Cluster.
	DefaultClusterPort = 17946
)

// Cluster is a management type for Serf which is used to maintain cluster membership of lbry nodes.
type Cluster struct {
	name     string
	port     int
	seedAddr string

	s       *serf.Serf
	eventCh chan serf.Event
	stop    *stopOnce.Stopper
}

// New returns a new Cluster instance that is not connected.
func New(port int, seedAddr string) *Cluster {
	return &Cluster{
		name:     crypto.RandString(12),
		port:     port,
		seedAddr: seedAddr,
		stop:     stopOnce.New(),
	}
}

// Start Initializes the Cluster based on a configuration passed via the New function. It then stores the seed
// address, starts gossiping and listens for gossip.
func (c *Cluster) Connect() error {
	var err error

	conf := serf.DefaultConfig()
	conf.MemberlistConfig.BindPort = c.port
	conf.MemberlistConfig.AdvertisePort = c.port
	conf.NodeName = c.name

	nullLogger := baselog.New(ioutil.Discard, "", 0)
	conf.Logger = nullLogger

	c.eventCh = make(chan serf.Event)
	conf.EventCh = c.eventCh

	c.s, err = serf.Create(conf)
	if err != nil {
		return errors.Prefix("couldn't create cluster", err)
	}

	if c.seedAddr != "" {
		_, err = c.s.Join([]string{c.seedAddr}, true)
		if err != nil {
			return err
		}
	}
	c.stop.Add(1)
	go func() {
		defer c.stop.Done()
		c.listen()
	}()
	return nil
}

// Shutdown safely shuts down the cluster.
func (c *Cluster) Shutdown() {
	c.stop.StopAndWait()
	if err := c.s.Leave(); err != nil {
		log.Error("error shutting down cluster - ", err)
	}
}

func (c *Cluster) listen() {
	for {
		select {
		case <-c.stop.Ch():
			return
		case event := <-c.eventCh:
			switch event.EventType() {
			case serf.EventMemberJoin, serf.EventMemberFailed, serf.EventMemberLeave:
				memberEvent := event.(serf.MemberEvent)
				if event.EventType() == serf.EventMemberJoin && len(memberEvent.Members) == 1 && memberEvent.Members[0].Name == c.name {
					// ignore event from my own joining of the cluster
					continue
				}

				//spew.Dump(c.Members())
				alive := getAliveMembers(c.s.Members())
				log.Printf("%s: my hash range is now %d of %d\n", c.name, getHashRangeStart(c.name, alive), len(alive))
				// figure out my new hash range based on the start and the number of alive members
				// get hashes in that range that need announcing
				// announce them
				// if more than one node is announcing each hash, figure out how to deal with last_announced_at so both nodes dont announce the same thing at the same time
			}
		}
	}
}

func getHashRangeStart(myName string, members []serf.Member) int {
	var names []string
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
	var alive []serf.Member
	for _, m := range members {
		if m.Status == serf.StatusAlive {
			alive = append(alive, m)
		}
	}
	return alive
}
