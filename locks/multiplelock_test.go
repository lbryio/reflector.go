package locks

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/stop"
)

var lock = NewMultipleLock()

func TestNewMultipleLock(t *testing.T) {
	grp := stop.New()
	for i := 0; i < 100; i++ {
		grp.Add(2)
		go doRWWork(i, i%10, grp)
		go doRWork(i, i%10, grp)
	}
	time.Sleep(5 * time.Second)
	grp.StopAndWait()
}
func doRWWork(worker int, resource int, grp *stop.Group) {
	for {
		select {
		case <-grp.Ch():
			grp.Done()
			return
		default:
			//log.Printf("RW - worker %d doing work on resource %d\n", worker, resource)
			lock.Lock(strconv.Itoa(resource))
			randomTime := time.Duration(rand.Int()%10+1) * time.Microsecond
			time.Sleep(randomTime)
			//log.Printf("RW - worker %d releasing %d\n", worker, resource)
			lock.Unlock(strconv.Itoa(resource))
		}
	}
}

func doRWork(worker int, resource int, grp *stop.Group) {
	for {
		select {
		case <-grp.Ch():
			grp.Done()
			return
		default:
			//log.Printf("R  - worker %d doing work on resource %d\n", worker, resource)
			lock.RLock(strconv.Itoa(resource))
			randomTime := time.Duration(rand.Int()%10+1) * time.Microsecond
			time.Sleep(randomTime)
			//log.Printf("R  - worker %d releasing %d\n", worker, resource)
			lock.RUnlock(strconv.Itoa(resource))
		}
	}
}
