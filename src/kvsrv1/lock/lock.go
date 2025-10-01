package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	ls string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, ls: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		err := lk.ck.Put(lk.ls, "locked", 0)
		if err == rpc.OK {
			return
		}
		// loop until the key's released
		value, version, err := lk.ck.Get(lk.ls)
		if err == rpc.OK && value == "" {
			err := lk.ck.Put(lk.ls, "locked", version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	_, version, _ := lk.ck.Get(lk.ls)
	lk.ck.Put(lk.ls, "", version)
}
