package reflector

import (
	"strconv"

	"github.com/lbryio/lbry.go/stopOnce"
	"github.com/lbryio/reflector.go/cluster"
	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"
)

// Prism is the root instance of the application and houses the DHT, Peer Server, Reflector Server, and Cluster.
type Prism struct {
	dht       *dht.DHT
	peer      *peer.Server
	reflector *Server
	cluster   *cluster.Cluster

	stop *stopOnce.Stopper
}

// NewPrism returns an initialized Prism instance pointer.
func NewPrism(store store.BlobStore, clusterSeedAddr string) *Prism {
	d, err := dht.New(nil)
	if err != nil {
		panic(err)
	}
	return &Prism{
		dht:       d,
		peer:      peer.NewServer(store),
		reflector: NewServer(store),
		cluster:   cluster.New(cluster.DefaultClusterPort, clusterSeedAddr),
		stop:      stopOnce.New(),
	}
}

// Start starts the components of the application.
func (p *Prism) Start() error {
	err := p.dht.Start()
	if err != nil {
		return err
	}

	err = p.cluster.Connect()
	if err != nil {
		return err
	}

	err = p.peer.Start("localhost:" + strconv.Itoa(peer.DefaultPort))
	if err != nil {
		return err
	}

	err = p.reflector.Start("localhost:" + strconv.Itoa(DefaultPort))
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
