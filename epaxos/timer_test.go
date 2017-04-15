package paxos

import "testing"

func TestTickingTimer(t *testing.T) {
	flag := false
	assertFlag := func(exp bool) {
		if flag != exp {
			t.Fatalf("expected flag in state %t, found %t", exp, flag)
		}
	}

	timer := makeTickingTimer(2, func() { flag = true })
	assertTimerSet := func(exp bool) {
		if a := timer.isSet(); a != exp {
			t.Fatalf("expected timer set = %t, found %t", exp, a)
		}
	}

	// The timer will tick until it reaches its timeout, upon which time
	// it will call its function.
	timer.tick()
	assertFlag(false)
	assertTimerSet(true)
	timer.tick()
	assertFlag(true)
	assertTimerSet(false)

	// The timer wont tick again until it is reset.
	flag = false
	timer.tick()
	assertFlag(false)
	assertTimerSet(false)

	// Upon reset, it will begin ticking again.
	timer.reset()
	assertTimerSet(true)
	timer.tick()
	assertFlag(false)
	assertTimerSet(true)
	timer.tick()
	assertFlag(true)
	assertTimerSet(false)

	// Upon stop, it will stop ticking until it is reset.
	flag = false
	timer.reset()
	assertTimerSet(true)
	timer.tick()
	assertFlag(false)
	assertTimerSet(true)
	timer.stop()
	assertTimerSet(false)
	timer.tick()
	assertFlag(false)
	assertTimerSet(false)
	timer.reset()
	assertTimerSet(true)
	timer.tick()
	assertFlag(false)
	assertTimerSet(true)
	timer.tick()
	assertFlag(true)
	assertTimerSet(false)
}
