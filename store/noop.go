package store

import "github.com/lbryio/lbry.go/v2/stream"

// NoopStore is a store that does nothing
type NoopStore struct{}

const nameNoop = "noop"

func (n *NoopStore) Name() string                        { return nameNoop }
func (n *NoopStore) Has(_ string) (bool, error)          { return false, nil }
func (n *NoopStore) Get(_ string) (stream.Blob, error)   { return nil, nil }
func (n *NoopStore) Put(_ string, _ stream.Blob) error   { return nil }
func (n *NoopStore) PutSD(_ string, _ stream.Blob) error { return nil }
func (n *NoopStore) Delete(_ string) error               { return nil }
func (n *NoopStore) Shutdown()                           { return }
