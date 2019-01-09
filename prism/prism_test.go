package prism

import (
	"math/big"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/lbry.go/dht/bits"
)

func TestAnnounceRange(t *testing.T) {

	t.Skip("TODO: this needs to actually test the thing")

	total := 17

	max := bits.MaxP().Big()
	interval := bits.MaxP().Big()
	spew.Dump(interval)
	interval.Div(interval, big.NewInt(int64(total)))

	for i := 0; i < total; i++ {
		start := big.NewInt(0).Mul(interval, big.NewInt(int64(i)))
		end := big.NewInt(0).Add(start, interval)
		if i == total-1 {
			end = end.Add(end, big.NewInt(10000)) // there are rounding issues sometimes, so lets make sure we get the full range
		}
		if end.Cmp(max) > 0 {
			end.Set(max)
		}

		spew.Dump(i, start, end, bits.FromBigP(start).Hex(), bits.FromBigP(end).Hex())
	}

	//startB := bits.FromBigP(start)
	//endB := bits.FromBigP(end)
	//
	//t.Logf("%s to %s\n", startB.Hex(), endB.Hex())

}
