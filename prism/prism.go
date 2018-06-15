package prism

import (
	"context"
	"math/big"
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

	DB    *db.SQL
	Blobs store.BlobStore
}

// DefaultConf returns a default config
func DefaultConf() *Config {
	return &Config{
		ClusterPort: cluster.DefaultClusterPort,
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
	dhtConf.SeedNodes = conf.DhtSeedNodes
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

	c.OnHashRangeChange = func(n, total int) {
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
	if p.conf.DB == nil {
		return errors.Err("db required in conf")
	}

	if p.conf.Blobs == nil {
		return errors.Err("blobs required in conf")
	}

	err := p.dht.Start()
	if err != nil {
		return err
	}

	err = p.cluster.Connect()
	if err != nil {
		return err
	}

	// TODO: should not be localhost forever. should prolly be 0.0.0.0, or configurable
	err = p.peer.Start("localhost:" + strconv.Itoa(p.conf.PeerPort))
	if err != nil {
		return err
	}

	// TODO: should not be localhost forever. should prolly be 0.0.0.0, or configurable
	err = p.reflector.Start("localhost:" + strconv.Itoa(p.conf.ReflectorPort))
	if err != nil {
		return err
	}

	return nil
}

// Shutdown gracefully shuts down the different prism components before exiting.
func (p *Prism) Shutdown() {
	p.stop.StopAndWait()
	p.reflector.Shutdown()
	p.peer.Shutdown()
	p.cluster.Shutdown()
	p.dht.Shutdown()
}

// AnnounceRange announces the `n`th interval of hashes, out of a total of `total` intervals
func (p *Prism) AnnounceRange(n, total int) {
	// TODO: if more than one node is announcing each hash, figure out how to deal with last_announced_at so both nodes dont announce the same thing at the same time

	// num and total are 1-indexed
	if n < 1 {
		log.Errorf("%s: n must be >= 1", p.dht.ID().HexShort())
		return
	}

	max := bits.MaxP().Big()
	interval := new(big.Int).Div(max, big.NewInt(int64(total)))

	start := new(big.Int).Mul(interval, big.NewInt(int64(n-1)))
	end := new(big.Int).Add(start, interval)
	if n == total {
		end = end.Add(end, big.NewInt(10000)) // there are rounding issues sometimes, so lets make sure we get the full range
	}
	if end.Cmp(max) > 0 {
		end.Set(max)
	}

	log.Debugf("%s: hash range is now %s to %s\n", p.dht.ID().HexShort(), bits.FromBigP(start).Hex(), bits.FromBigP(end).Hex())

	ctx, cancel := context.WithCancel(context.Background())
	hashCh, errCh := p.db.GetHashesInRange(ctx, bits.FromBigP(start), bits.FromBigP(end))

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
				p.dht.Add(hash)
			}
		}
	}()

	wg.Wait()
}
