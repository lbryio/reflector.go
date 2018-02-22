package cluster

import (
	"github.com/lbryio/lbry.go/errors"

	"github.com/hashicorp/serf/serf"
	log "github.com/sirupsen/logrus"
)

func Connect(nodeName, addr string, port int) (*serf.Serf, <-chan serf.Event, error) {
	conf := serf.DefaultConfig()
	conf.MemberlistConfig.BindPort = port
	conf.MemberlistConfig.AdvertisePort = port
	conf.NodeName = nodeName

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
