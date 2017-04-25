package epaxos

// tickingTimer is a timer that is not linked to physical time, but instead is
// controlled by calling its tick method. Using this timer allows timer state
// to be manipulated externally. When the timer goes off, it will call its
// onTimeout callback.
type tickingTimer struct {
	timeout      int
	ticksElapsed int
	paused       bool
	onTimeout    func()
}

func makeTickingTimer(timeout int, onTimeout func()) tickingTimer {
	return tickingTimer{
		timeout:   timeout,
		onTimeout: onTimeout,
	}
}

func (t *tickingTimer) tick() {
	if t.paused {
		return
	}

	t.ticksElapsed++
	if t.ticksElapsed >= t.timeout {
		t.paused = true
		t.onTimeout()
	}
}

func (t *tickingTimer) reset() {
	t.paused = false
	t.ticksElapsed = 0
}

func (t *tickingTimer) resetWithJitter(jitter int) {
	t.paused = false
	t.ticksElapsed = jitter
}

func (t *tickingTimer) stop() {
	t.paused = true
	t.ticksElapsed = 0
}

func (t *tickingTimer) isSet() bool {
	return !t.paused
}

func (t *tickingTimer) instrument(instrumentedTimeout func()) {
	old := t.onTimeout
	t.onTimeout = func() {
		instrumentedTimeout()
		old()
	}
}
