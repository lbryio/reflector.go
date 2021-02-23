package shared

import "github.com/lbryio/lbry.go/v2/extras/errors"

//ErrNotImplemented is a standard error when a store that implements the store interface does not implement a method
var ErrNotImplemented = errors.Base("this store does not implement this method")
