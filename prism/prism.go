package prism

import (
	"context"
	"strconv"
	"sync"

	"github.com/lbryio/reflector.go/cluster"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/dht/bits"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/stopOnce"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	PeerPort      int
	ReflectorPort int

	DhtAddress   string
	DhtSeedNodes []string

	ClusterPort     int
	ClusterSeedAddr string

	// limit the range of hashes to announce. useful for testing
	HashRange *bits.Range

	DB    *db.SQL
	Blobs store.BlobStore
}

// DefaultConf returns a default config
func DefaultConf() *Config {
	return &Config{
		ClusterPort: cluster.DefaultPort,
	}
}

// Prism is the root instance of the application and houses the DHT, Peer Server, Reflector Server, and Cluster.
type Prism struct {
	conf *Config

	db        *db.SQL
	dht       *dht.DHT
	peer      *peer.Server
	reflector *reflector.Server
	cluster   *cluster.Cluster

	stop *stopOnce.Stopper
}

// New returns an initialized Prism instance
func New(conf *Config) *Prism {
	if conf == nil {
		conf = DefaultConf()
	}

	dhtConf := dht.NewStandardConfig()
	dhtConf.Address = conf.DhtAddress
	dhtConf.PeerProtocolPort = conf.PeerPort
	if len(conf.DhtSeedNodes) > 0 {
		dhtConf.SeedNodes = conf.DhtSeedNodes
	}
	d := dht.New(dhtConf)

	c := cluster.New(conf.ClusterPort, conf.ClusterSeedAddr)

	p := &Prism{
		conf: conf,

		db:        conf.DB,
		dht:       d,
		cluster:   c,
		peer:      peer.NewServer(conf.Blobs),
		reflector: reflector.NewServer(conf.Blobs),

		stop: stopOnce.New(),
	}

	c.OnMembershipChange = func(n, total int) {
		p.stop.Add(1)
		go func() {
			p.AnnounceRange(n, total)
			p.stop.Done()
		}()
	}

	return p
}

// Start starts the components of the application.
func (p *Prism) Start() error {
	var err error

	if p.conf.DB == nil {
		return errors.Err("db required in conf")
	}

	if p.conf.Blobs == nil {
		return errors.Err("blobs required in conf")
	}

	err = p.peer.Start(":" + strconv.Itoa(p.conf.PeerPort))
	if err != nil {
		return err
	}

	err = p.reflector.Start(":" + strconv.Itoa(p.conf.ReflectorPort))
	if err != nil {
		return err
	}

	err = p.dht.Start()
	if err != nil {
		return err
	}

	err = p.cluster.Connect()
	if err != nil {
		return err
	}

	return nil
}

// Shutdown gracefully shuts down the different prism components before exiting.
func (p *Prism) Shutdown() {
	p.stop.StopAndWait()
	p.cluster.Shutdown()
	p.dht.Shutdown()
	p.reflector.Shutdown()
	p.peer.Shutdown()
}

// AnnounceRange announces the `n`th interval of hashes, out of a total of `total` intervals
func (p *Prism) AnnounceRange(n, total int) {
	// TODO: if more than one node is announcing each hash, figure out how to deal with last_announced_at so both nodes dont announce the same thing at the same time

	// num and total are 1-indexed
	if n < 1 {
		log.Errorf("%s: n must be >= 1", p.dht.ID().HexShort())
		return
	}

	var r bits.Range
	if p.conf.HashRange != nil {
		r = *p.conf.HashRange
	} else {
		//r := bits.MaxRange().IntervalP(n, total)
		// TODO: this is temporary. it lets me test with a small number of hashes. use the full range in production
		min, max, err := p.db.GetHashRange()
		if err != nil {
			log.Errorf("%s: error getting hash range: %s", p.dht.ID().HexShort(), err.Error())
			return
		}
		r = (bits.Range{Start: bits.FromHexP(min), End: bits.FromHexP(max)}).IntervalP(n, total)
	}

	log.Infof("%s: hash range is now %s to %s", p.dht.ID().HexShort(), r.Start, r.End)

	ctx, cancel := context.WithCancel(context.Background())
	hashCh, errCh := p.db.GetHashesInRange(ctx, r.Start, r.End)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-p.stop.Ch():
			return
		case err, more := <-errCh:
			if more && err != nil {
				log.Error(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-p.stop.Ch():
				cancel()
				return
			case hash, more := <-hashCh:
				if !more {
					return
				}
				//log.Infof("%s: announcing %s", p.dht.ID().HexShort(), hash.Hex())
				p.dht.Add(hash)
			}
		}
	}()

	wg.Wait()
}
