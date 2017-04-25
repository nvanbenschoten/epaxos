package epaxos

import (
	"context"
	"errors"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

var (
	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("paxos: stopped")
)

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	Messages []pb.Message

	// ExecutedCommands specifies commands to be executed by a state-machine.
	// These have previously been committed to stable store.
	ExecutedCommands []pb.Command
}

// containsUpdates returns whether the Ready struct contains any updates that
// need to be acted upon.
func (rd Ready) containsUpdates() bool {
	return len(rd.Messages) > 0 || len(rd.ExecutedCommands) > 0
}

// Node represents a node in a paxos cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick.
	// Election timeouts and progress timeouts are in units of ticks.
	Tick()
	// Propose proposes that data be ordered by paxos.
	Propose(ctx context.Context, command pb.Command) error
	// Step advances the state machine using the given message. ctx.Err() will be
	// returned, if any.
	Step(ctx context.Context, msg pb.Message) error
	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by
	// Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all
	// committed entries and snapshots from the previous one have finished.
	Ready() <-chan Ready
	// Stop performs any necessary termination of the Node.
	Stop()
}

// StartNode returns a new Node with the given configuration.
func StartNode(c *Config) Node {
	return RestartNode(c)
}

// RestartNode returns a new Node with the given configuration and the
// PersistentState applied.
func RestartNode(c *Config) Node {
	p := newEPaxos(c)
	n := makeNode()
	n.logger = c.Logger
	go n.run(p)
	return &n
}

// node is the canonical implementation of the Node interface. It provides a
// thread-safe handle around the thread-unsafe paxos object.
type node struct {
	propc  chan pb.Command
	msgc   chan pb.Message
	readyc chan Ready
	tickc  chan struct{}
	done   chan struct{}
	stop   chan struct{}

	logger Logger
}

func makeNode() node {
	return node{
		propc:  make(chan pb.Command),
		msgc:   make(chan pb.Message),
		readyc: make(chan Ready),
		// buffered chan, so paxos node can buffer some ticks when the node is
		// busy processing messages. Paxos node will resume process buffered
		// ticks when it becomes idle.
		tickc: make(chan struct{}, 128),
		done:  make(chan struct{}),
		stop:  make(chan struct{}),
	}
}

func (n *node) run(p *epaxos) {
	for {
		var readyc chan Ready
		rd := makeReady(p)
		if rd.containsUpdates() {
			readyc = n.readyc
		}

		select {
		case <-n.tickc:
			p.Tick()
		case cmd := <-n.propc:
			p.Request(cmd)
		case m := <-n.msgc:
			p.Step(m)
		case readyc <- rd:
			p.clearMsgs()
			p.clearExecutedCommands()
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick implements the Node interface.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.logger.Warningf("A tick missed to fire. Node blocking for too long!")
	}
}

// Propose implements the Node interface.
func (n *node) Propose(ctx context.Context, cmd pb.Command) error {
	select {
	case n.propc <- cmd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// Step implements the Node interface.
func (n *node) Step(ctx context.Context, m pb.Message) error {
	select {
	case n.msgc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// Ready implements the Node interface.
func (n *node) Ready() <-chan Ready {
	return n.readyc
}

func makeReady(p *epaxos) Ready {
	return Ready{
		Messages:         p.msgs,
		ExecutedCommands: p.executedCmds,
	}
}

// Stop implements the Node interface.
func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it.
	case <-n.done:
		// Node has already been stopped - no need to do anything.
		return
	}
	// Block until the stop has been acknowledged by run().
	<-n.done
}
