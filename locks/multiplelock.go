package locks

import (
	"fmt"
	"sync"
	"time"
)

// MultipleLock is the main interface for multiLock based on key
type MultipleLock interface {
	Lock(key string)
	RLock(key string)
	Unlock(key string)
	RUnlock(key string)
}

func NewMultipleLock() MultipleLock {
	return &multiLock{
		locks: make(map[string]*itemLock),
		mu:    sync.Mutex{},
	}
}

type itemLock struct {
	lk  *sync.RWMutex
	cnt int64
}

// multiLock is an optimized locking system per locking key
type multiLock struct {
	locks map[string]*itemLock
	mu    sync.Mutex // synchronize reads/writes to locks map
}

func debugPrint(format string, a ...interface{}) {
	debugEnabled := false
	if debugEnabled {
		a = append(a, time.Now().Format("04:05.000000"))
		fmt.Printf(format+" at %s\n", a...)
	}
}

func (ml *multiLock) Lock(key string) {
	debugPrint("mutex requested %s", key)
	ml.mu.Lock()
	debugPrint("mutex acquired %s", key)

	itmLock, exists := ml.locks[key]
	if !exists {
		debugPrint("new lock created %s", key)
		itmLock = &itemLock{&sync.RWMutex{}, 0}
		ml.locks[key] = itmLock
	}
	itmLock.cnt++
	debugPrint("releasing mutex %s", key)
	ml.mu.Unlock()

	debugPrint("Lock requested %s", key)
	itmLock.lk.Lock()
	debugPrint("Lock acquired %s", key)
}

func (ml *multiLock) RLock(key string) {
	debugPrint("mutex requested %s", key)
	ml.mu.Lock()
	debugPrint("mutex acquired %s", key)

	itmLock, exists := ml.locks[key]
	if !exists {
		debugPrint("new lock created for %s", key)
		itmLock = &itemLock{&sync.RWMutex{}, 0}
		ml.locks[key] = itmLock
	}
	itmLock.cnt++
	debugPrint("releasing mutex %s", key)
	ml.mu.Unlock()

	debugPrint("RLock requested %s", key)
	itmLock.lk.RLock()
	debugPrint("RLock acquired %s", key)
}

func (ml *multiLock) Unlock(key string) {
	debugPrint("mutex requested %s", key)
	ml.mu.Lock()
	debugPrint("mutex acquired %s", key)

	itmLock, exists := ml.locks[key]
	if !exists {
		panic("sync Unlock of non existent lock!!")
	}

	debugPrint("Unlock %s", key)
	itmLock.lk.Unlock()
	itmLock.cnt--
	if itmLock.cnt == 0 {
		debugPrint("delete lock %s", key)
		delete(ml.locks, key)
	}
	if itmLock.cnt < 0 {
		panic("sync Unlock of free Lock!!")
	}

	debugPrint("releasing mutex %s", key)
	ml.mu.Unlock()
}

func (ml *multiLock) RUnlock(key string) {
	debugPrint("mutex requested %s", key)
	ml.mu.Lock()
	debugPrint("mutex acquired %s", key)

	itmLock, exists := ml.locks[key]
	if !exists {
		panic("sync Unlock of non existent lock!!")
	}

	debugPrint("RUnlock %s", key)
	itmLock.lk.RUnlock()
	itmLock.cnt--
	if itmLock.cnt == 0 {
		debugPrint("delete lock %s", key)
		delete(ml.locks, key)
	}
	if itmLock.cnt < 0 {
		panic("sync Unlock of free Lock!!")
	}

	debugPrint("releasing mutex %s", key)
	ml.mu.Unlock()
}
