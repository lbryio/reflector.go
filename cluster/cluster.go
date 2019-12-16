package cluster

import (
	"io/ioutil"
	baselog "log"
	"sort"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/crypto"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultPort                  = 17946
	MembershipChangeBufferWindow = 1 * time.Second
)

// Cluster maintains cluster membership and notifies on certain events
type Cluster struct {
	OnMembershipChange func(n, total int)

	name     string
	port     int
	seedAddr string

	s       *serf.Serf
	eventCh chan serf.Event
	stop    *stop.Group
}

// New returns a new Cluster instance that is not connected.
func New(port int, seedAddr string) *Cluster {
	return &Cluster{
		name:     crypto.RandString(12),
		port:     port,
		seedAddr: seedAddr,
		stop:     stop.New(),
	}
}

// Connect Initializes the Cluster based on a configuration passed via the New function. It then stores the seed
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
		c.listen()
		c.stop.Done()
	}()

	log.Debugf("cluster started")
	return nil
}

// Shutdown safely shuts down the cluster.
func (c *Cluster) Shutdown() {
	log.Debug("shutting down cluster...")
	c.stop.StopAndWait()
	err := c.s.Leave()
	if err != nil {
		log.Error(errors.Prefix("shutting down cluster", err))
	}
	log.Debugf("cluster stopped")
}

func (c *Cluster) listen() {
	var timerCh <-chan time.Time
	timer := time.NewTimer(0)

	for {
		select {
		case <-c.stop.Ch():
			return
		case event := <-c.eventCh:
			switch event.EventType() {
			case serf.EventMemberJoin, serf.EventMemberFailed, serf.EventMemberLeave:
				//	// ignore event from my own joining of the cluster
				//memberEvent := event.(serf.MemberEvent)
				//if event.EventType() == serf.EventMemberJoin && len(memberEvent.Members) == 1 && memberEvent.Members[0].Name == c.name {
				//	continue
				//}

				if timerCh == nil {
					timer.Reset(MembershipChangeBufferWindow)
					timerCh = timer.C
				}
			}
		case <-timerCh:
			if c.OnMembershipChange != nil {
				alive := getAliveMembers(c.s.Members())
				c.OnMembershipChange(getHashInterval(c.name, alive), len(alive))
			}
			timerCh = nil
		}
	}
}

func getHashInterval(myName string, members []serf.Member) int {
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
