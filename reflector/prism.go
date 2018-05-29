package reflector

import (
	"github.com/lbryio/lbry.go/stopOnce"
	"github.com/lbryio/reflector.go/cluster"
	"github.com/lbryio/reflector.go/dht"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"
)

type Prism struct {
	dht       *dht.DHT
	peer      *peer.Server
	reflector *Server
	cluster   *cluster.Cluster

	stop *stopOnce.Stopper
}

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

func (p *Prism) Connect() error {
	err := p.dht.Start()
	if err != nil {
		return err
	}

	err = p.cluster.Connect()
	if err != nil {
		return err
	}

	// start peer

	// start reflector

	return nil
}

func (p *Prism) Shutdown() {
	p.stop.StopAndWait()
	p.reflector.Shutdown()
	p.peer.Shutdown()
	p.cluster.Shutdown()
	p.dht.Shutdown()
}
