package epaxos

import (
	"math/rand"
	"reflect"
	"testing"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

func TestConfig(t *testing.T) {
	l := NewDefaultLogger()
	c := &Config{
		ID:     12,
		Nodes:  []pb.ReplicaID{2, 5, 12, 77},
		Logger: l,
	}
	p := newEPaxos(c)

	if p.id != c.ID {
		t.Errorf("expected Paxos node ID %d, found %d", c.ID, p.id)
	}
	if !reflect.DeepEqual(p.nodes, c.Nodes) {
		t.Errorf("expected Paxos nodes %c, found %d", c.Nodes, p.nodes)
	}
	if p.logger != l {
		t.Errorf("expected Paxos logger %p, found %p", l, p.logger)
	}
}

func (p *epaxos) ReadMessages() []pb.Message {
	msgs := p.msgs
	p.clearMsgs()
	return msgs
}

type conn struct {
	from, to pb.ReplicaID
}

type network struct {
	peers    map[pb.ReplicaID]*epaxos
	failures map[*epaxos]struct{}
	dropm    map[conn]float64
}

func newNetwork(nodeCount int) network {
	peers := make(map[pb.ReplicaID]*epaxos, nodeCount)
	peersSlice := make([]pb.ReplicaID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		peersSlice[i] = pb.ReplicaID(i)
	}
	for _, r := range peersSlice {
		peers[r] = newEPaxos(&Config{
			ID:       r,
			Nodes:    peersSlice,
			RandSeed: int64(r),
		})
	}
	return network{
		peers:    peers,
		failures: make(map[*epaxos]struct{}, nodeCount),
		dropm:    make(map[conn]float64),
	}
}

func (n *network) crash(id pb.ReplicaID) *epaxos {
	p := n.peers[id]
	n.failures[p] = struct{}{}
	return p
}

func (n *network) alive(p *epaxos) bool {
	_, failed := n.failures[p]
	return !failed
}

func (n *network) drop(from, to pb.ReplicaID, perc float64) {
	n.dropm[conn{from: from, to: to}] = perc
}

func (n *network) dropForAll(perc float64) {
	for from := range n.peers {
		for to := range n.peers {
			if from != to {
				n.drop(from, to, perc)
			}
		}
	}
}

func (n *network) cut(one, other pb.ReplicaID) {
	n.drop(one, other, 1.0)
	n.drop(other, one, 1.0)
}

func (n *network) isolate(id pb.ReplicaID) {
	for other := range n.peers {
		if other != id {
			n.cut(id, other)
		}
	}
}

func (n *network) tickAll() {
	for _, p := range n.peers {
		if n.alive(p) {
			p.Tick()
		}
	}
}

func (n *network) deliverAllMessages() {
	var msgs []pb.Message
	for _, p := range n.peers {
		if n.alive(p) {
			newMsgs := p.ReadMessages()
			for _, msg := range newMsgs {
				msgConn := conn{from: p.id, to: msg.To}
				perc := n.dropm[msgConn]
				if perc > 0 {
					if n := rand.Float64(); n < perc {
						continue
					}
				}
				msgs = append(msgs, msg)
			}
		}
	}
	for _, msg := range msgs {
		dest := n.peers[msg.To]
		if n.alive(dest) {
			dest.Step(msg)
		}
	}
}

func (n *network) quorumHas(pred func(*epaxos) bool) bool {
	count := 0
	for _, p := range n.peers {
		if pred(p) {
			count++
		}
	}
	return count > int(len(n.peers)/2)
}

func (n *network) waitExecuteInstance(inst *instance) bool {
	const maxTicksPerInstanceExecution = 10
	for i := 0; i < maxTicksPerInstanceExecution; i++ {
		n.tickAll()
		n.deliverAllMessages()
		if n.quorumHas(func(p *epaxos) bool {
			return p.hasExecuted(inst.r, inst.i)
		}) {
			return true
		}
	}
	return false
}

func TestExecuteCommandNoFailures(t *testing.T) {
	n := newNetwork(5)

	for _, peer := range n.peers {
		cmd := makeTestingCommand("a", "z")
		inst := peer.onRequest(cmd)

		if !n.waitExecuteInstance(inst) {
			t.Fatalf("command execution failed, command %+v never installed", cmd)
		}
	}
}

func TestExecuteCommandMinorityFailures(t *testing.T) {
	n := newNetwork(5)
	n.crash(0)
	n.crash(1)

	for _, peer := range n.peers {
		if n.alive(peer) {
			cmd := makeTestingCommand("a", "z")
			inst := peer.onRequest(cmd)

			if !n.waitExecuteInstance(inst) {
				t.Fatalf("command execution failed, command %+v never installed", cmd)
			}
		}
	}
}

func TestExecuteCommandMajorityFailures(t *testing.T) {
	n := newNetwork(5)
	n.crash(0)
	n.crash(1)
	n.crash(2)

	for _, peer := range n.peers {
		if n.alive(peer) {
			cmd := makeTestingCommand("a", "z")
			inst := peer.onRequest(cmd)

			if n.waitExecuteInstance(inst) {
				t.Fatalf("command execution succeeded with minority of nodes")
			}
		}
	}
}
