package epaxos

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/btree"

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

func (p *epaxos) ExecutableCommands() []pb.Command {
	cmds := p.executedCmds
	p.clearExecutedCommands()
	return cmds
}

type conn struct {
	from, to pb.ReplicaID
}

type network struct {
	peers       map[pb.ReplicaID]*epaxos
	failures    map[*epaxos]struct{}
	dropm       map[conn]float64
	interceptor func(pb.ReplicaID, pb.Message)
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

func (n *network) F() int {
	return n.peers[0].F()
}

func (n *network) quorum(val int) bool {
	return n.peers[0].quorum(val)
}

func (n *network) setInterceptor(f func(from pb.ReplicaID, msg pb.Message)) {
	n.interceptor = f
}

func (n *network) crash(id pb.ReplicaID) *epaxos {
	p := n.peers[id]
	n.failures[p] = struct{}{}
	return p
}

func (n *network) crashN(c int) {
	crashed := 0
	for r := range n.peers {
		if crashed >= c {
			return
		}
		n.crash(r)
		crashed++
	}
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
	for r, p := range n.peers {
		if n.alive(p) {
			newMsgs := p.ReadMessages()
			for _, msg := range newMsgs {
				if n.interceptor != nil {
					n.interceptor(r, msg)
				}
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

func (n *network) count(pred func(*epaxos) bool) int {
	count := 0
	for _, p := range n.peers {
		if pred(p) {
			count++
		}
	}
	return count
}

func (n *network) quorumHas(pred func(*epaxos) bool) bool {
	return n.quorum(n.count(pred))
}

func (n *network) allHave(pred func(*epaxos) bool) bool {
	return len(n.peers) == n.count(pred)
}

// waitExecuteInstance waits until the given instance has executed.
// If quorum is true, it will wait until the instance is executed on
// a quorum of nodes. If it is true, it will wait until the instance
// is executed on all nodes.
func (n *network) waitExecuteInstance(inst *instance, quorum bool) bool {
	waitUntil := n.allHave
	if quorum {
		waitUntil = n.quorumHas
	}

	const maxTicksPerInstanceExecution = 10
	for i := 0; i < maxTicksPerInstanceExecution; i++ {
		n.tickAll()
		n.deliverAllMessages()
		if waitUntil(func(p *epaxos) bool {
			return p.hasExecuted(inst.is.ReplicaID, inst.is.InstanceNum)
		}) {
			return true
		}
	}
	return false
}

// TestExecuteCommandsNoFailures verifies that each replica can propose a
// command and that the command will be executed, in the case where there
// are no failures.
func TestExecuteCommandsNoFailures(t *testing.T) {
	n := newNetwork(5)

	for _, peer := range n.peers {
		cmd := newTestingCommand("a", "z")
		inst := peer.onRequest(cmd)

		if !n.waitExecuteInstance(inst, true /* quorum */) {
			t.Fatalf("command execution failed, instance %+v never installed", inst)
		}
	}
}

// TestExecuteCommandsNoFailures verifies that each replica can propose a
// command and that the command will be executed, in the case where there
// are F or fewer failures.
func TestExecuteCommandsMinorityFailures(t *testing.T) {
	n := newNetwork(5)
	n.crashN(n.F())

	for _, peer := range n.peers {
		if n.alive(peer) {
			cmd := newTestingCommand("a", "z")
			inst := peer.onRequest(cmd)

			if !n.waitExecuteInstance(inst, true /* quorum */) {
				t.Fatalf("command execution failed, instance %+v never installed", inst)
			}
		}
	}
}

// TestExecuteCommandsNoFailures verifies that no replica can make forward
// progress whether there are more than F failures.
func TestExecuteCommandsMajorityFailures(t *testing.T) {
	n := newNetwork(5)
	n.crashN(n.F() + 1)

	for _, peer := range n.peers {
		if n.alive(peer) {
			cmd := newTestingCommand("a", "z")
			inst := peer.onRequest(cmd)

			if n.waitExecuteInstance(inst, true /* quorum */) {
				t.Fatalf("command execution succeeded with minority of nodes")
			}
		}
	}
}

// TestExecuteCommandsOneRTTReads verifies that every command in a read only
// workload will be able to commit in 1 round-trip.
func TestExecuteCommandsOneRTTReads(t *testing.T) {
	n := newNetwork(5)
	n.setInterceptor(func(from pb.ReplicaID, msg pb.Message) {
		if _, ok := msg.Type.(*pb.Message_Accept); ok {
			t.Fatalf("Accept messages should never be sent")
		}
	})

	var insts []*instance
	for _, peer := range n.peers {
		cmd := newTestingReadCommand("a", "z")
		inst := peer.onRequest(cmd)
		insts = append(insts, inst)
	}
	for _, inst := range insts {
		if !n.waitExecuteInstance(inst, true /* quorum */) {
			t.Fatalf("command execution failed, instance %+v never installed", inst)
		}
	}
}

// TestExecuteCommandsOneRTTDifferentKeys verifies that every command in a
// non-interfering workload will be able to commit in 1 round-trip.
func TestExecuteCommandsOneRTTDifferentKeys(t *testing.T) {
	n := newNetwork(5)
	n.setInterceptor(func(from pb.ReplicaID, msg pb.Message) {
		if _, ok := msg.Type.(*pb.Message_Accept); ok {
			t.Fatalf("Accept messages should never be sent")
		}
	})

	var insts []*instance
	const letters = "abcde"
	for r, peer := range n.peers {
		cmd := newTestingCommand(letters[int(r):int(r)+1], "")
		inst := peer.onRequest(cmd)
		insts = append(insts, inst)
	}
	for _, inst := range insts {
		if !n.waitExecuteInstance(inst, true /* quorum */) {
			t.Fatalf("command execution failed, instance %+v never installed", inst)
		}
	}
}

// TestExecuteSerializableCommands verifies that in a workload where all commands
// interfere, all replicas will end up with identical instance spaces and identical
// command execution orders. This is not true of workloads where some commands do
// not interfere, because the non-interfering commands may be ordered differently
// by different replicas without a serializability violation.
func TestExecuteSerializableCommands(t *testing.T) {
	n := newNetwork(5)

	var insts []*instance
	for id, peer := range n.peers {
		if !(id == 0 || id == 1 || id == 2) {
			continue
		}
		cmd := newTestingCommand("a", "z")
		inst := peer.onRequest(cmd)
		insts = append(insts, inst)
	}
	for _, inst := range insts {
		if !n.waitExecuteInstance(inst, false /* all nodes */) {
			t.Fatalf("command execution failed, instance %+v never installed", inst)
		}
	}

	peer0 := n.peers[0]
	instSpace := makeInstSpaceComaparable(peer0.commands)
	execOrder := peer0.ExecutableCommands()
	for i, peer := range n.peers {
		if peer == peer0 {
			continue
		}
		otherInstSpace := makeInstSpaceComaparable(peer.commands)
		if !reflect.DeepEqual(instSpace, otherInstSpace) {
			t.Fatalf("peer %d: instance spaces differ: \n%+v\n%+v\n%+v", i, instSpace, otherInstSpace)
		}
		otherExecOrder := peer.ExecutableCommands()
		if !reflect.DeepEqual(execOrder, otherExecOrder) {
			t.Fatalf("peer %d: command execution orders differ: %+v vs %+v", i, execOrder, otherExecOrder)
		}
	}
}

func makeInstSpaceComaparable(
	instSpace map[pb.ReplicaID]*btree.BTree,
) map[pb.ReplicaID][]pb.InstanceState {
	cmpInstSpace := make(map[pb.ReplicaID][]pb.InstanceState, len(instSpace))
	for r, tree := range instSpace {
		cmpInstSpace[r] = treeToSlice(tree)
	}
	return cmpInstSpace
}

// treeToSlice converts a BTree constaining *instance elements to a slice
// containing pb.InstanceState elements. This is useful because two BTrees
// with the same contents may not have the same physical structure, which
// can throw off reflect.DeepEqual.
func treeToSlice(tree *btree.BTree) []pb.InstanceState {
	s := make([]pb.InstanceState, 0, tree.Len())
	tree.Ascend(func(i btree.Item) bool {
		s = append(s, i.(*instance).is)
		return true
	})
	return s
}
