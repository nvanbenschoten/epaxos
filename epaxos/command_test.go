package epaxos

import (
	"math/rand"
	"reflect"
	"testing"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

// newTestingCommand creates a writing *pb.Command with the provided start and
// end keys.
func newTestingCommand(start, end string) *pb.Command {
	return &pb.Command{
		ID: rand.Uint64(),
		Span: pb.Span{
			Key:    pb.Key(start),
			EndKey: pb.Key(end),
		},
		Writing: true,
	}
}

func newTestingReadCommand(start, end string) *pb.Command {
	cmd := newTestingCommand(start, end)
	cmd.Writing = false
	return cmd
}

// newTestingEPaxos creates a new epaxos state machine with the following
// structure:
//
// id: 0
// nodes: [0, 1 ,2]
// commands: {
//  0: [1: {{"a","z"}, 1}, 2: {{"a","m"}, 4}]
//  1: [1: {{"a","z"}, 2}, 2: {{"n","z"}, 5}]
//  2: [1: {{"a","b"}, 3}]
// }
func newTestingEPaxos() *epaxos {
	c := Config{ID: 0, Nodes: []pb.ReplicaID{0, 1, 2}}
	p := newEPaxos(&c)

	inst01 := p.newInstance(0, 1)
	inst01.is.InstanceData = pb.InstanceData{
		Command: newTestingCommand("a", "z"),
		SeqNum:  1,
		Deps:    []pb.InstanceID{},
	}

	inst11 := p.newInstance(1, 1)
	inst11.is.InstanceData = pb.InstanceData{
		Command: newTestingCommand("a", "z"),
		SeqNum:  2,
		Deps: []pb.InstanceID{
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
		},
	}

	inst21 := p.newInstance(2, 1)
	inst21.is.InstanceData = pb.InstanceData{
		Command: newTestingCommand("a", "b"),
		SeqNum:  3,
		Deps: []pb.InstanceID{
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		},
	}

	inst02 := p.newInstance(0, 2)
	inst02.is.InstanceData = pb.InstanceData{
		Command: newTestingCommand("a", "m"),
		SeqNum:  4,
		Deps: []pb.InstanceID{
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 2, InstanceNum: 1},
		},
	}

	inst12 := p.newInstance(1, 2)
	inst12.is.InstanceData = pb.InstanceData{
		Command: newTestingCommand("n", "z"),
		SeqNum:  5,
		Deps: []pb.InstanceID{
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		},
	}

	p.commands[0].ReplaceOrInsert(inst01)
	p.commands[1].ReplaceOrInsert(inst11)
	p.commands[2].ReplaceOrInsert(inst21)
	p.commands[0].ReplaceOrInsert(inst02)
	p.commands[1].ReplaceOrInsert(inst12)

	return p
}

// changeID changes the replica's ID. Used for testing to allow an epaxos
// state machine to act as other replicas.
func (p *epaxos) changeID(t *testing.T, newR pb.ReplicaID) {
	if !p.knownReplica(newR) {
		t.Fatalf("unknown replica %v", newR)
	}
	p.id = newR
}

func TestOnRequestIncrementInstanceNumber(t *testing.T) {
	p := newTestingEPaxos()

	// Verify current max instance numbers.
	expMaxInstanceNums := map[pb.ReplicaID]pb.InstanceNum{
		0: 2,
		1: 2,
		2: 1,
	}
	assertMaxInstanceNums := func() {
		for r, expMaxInst := range expMaxInstanceNums {
			if a, e := p.maxInstanceNum(r), expMaxInst; a != e {
				t.Errorf("expected max instance number %v for replica %v, found %v", e, r, a)
			}
		}
	}
	assertMaxInstanceNums()

	// Crete a new command for replica 0 and verify the new max instance number.
	newCmd := newTestingCommand("a", "z")
	p.onRequest(newCmd)
	expMaxInstanceNums[0] = 3
	assertMaxInstanceNums()

	// Crete a new command for replica 1 and verify the new max instance number.
	p.changeID(t, 1)
	p.onRequest(newCmd)
	expMaxInstanceNums[1] = 3
	assertMaxInstanceNums()

	// Crete a new command for replica 2 and verify the new max instance number.
	p.changeID(t, 2)
	p.onRequest(newCmd)
	expMaxInstanceNums[2] = 2
	assertMaxInstanceNums()
}

func TestOnRequestIncrementSequenceNumber(t *testing.T) {
	p := newTestingEPaxos()

	// Verify current max sequence numbers.
	expMaxSeqNums := map[pb.ReplicaID]pb.SeqNum{
		0: 4,
		1: 5,
		2: 3,
	}
	assertMaxSeqNums := func() {
		for r, expMaxSeq := range expMaxSeqNums {
			if a, e := p.maxSeqNum(r), expMaxSeq; a != e {
				t.Errorf("expected max seq number %v for replica %v, found %v", e, r, a)
			}
		}
	}
	assertMaxSeqNums()

	// Crete a new command for replica 0 and verify the new max seq number.
	newCmd := newTestingCommand("a", "z")
	p.onRequest(newCmd)
	expMaxSeqNums[0] = 6
	assertMaxSeqNums()

	// Crete a new command for replica 1 and verify the new max seq number.
	p.changeID(t, 1)
	p.onRequest(newCmd)
	expMaxSeqNums[1] = 7
	assertMaxSeqNums()

	// Crete a new command for replica 2 and verify the new max seq number.
	p.changeID(t, 2)
	p.onRequest(newCmd)
	expMaxSeqNums[2] = 8
	assertMaxSeqNums()
}

func TestOnRequestDependencies(t *testing.T) {
	p := newTestingEPaxos()

	// Verify current max dependencies numbers.
	expMaxDeps := map[pb.ReplicaID][]pb.InstanceID{
		0: {
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 2, InstanceNum: 1},
		},
		1: {
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		},
		2: {
			pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
			pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		},
	}
	assertMaxDeps := func() {
		for r, expDeps := range expMaxDeps {
			if a, e := p.maxDeps(r), expDeps; !reflect.DeepEqual(a, e) {
				t.Errorf("expected max deps %+v for replica %v, found %+v", e, r, a)
			}
		}
	}
	assertMaxDeps()

	// Crete a new command for replica 0 and verify the new max deps.
	newCmd := newTestingCommand("a", "z")
	p.onRequest(newCmd)
	expMaxDeps[0] = []pb.InstanceID{
		pb.InstanceID{ReplicaID: 0, InstanceNum: 1},
		pb.InstanceID{ReplicaID: 0, InstanceNum: 2},
		pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		pb.InstanceID{ReplicaID: 1, InstanceNum: 2},
		pb.InstanceID{ReplicaID: 2, InstanceNum: 1},
	}
	assertMaxDeps()

	// Crete a new command for replica 1 and verify the new max deps.
	newCmd.Span.Key = pb.Key("c")
	p.changeID(t, 1)
	p.onRequest(newCmd)
	expMaxDeps[1] = []pb.InstanceID{
		pb.InstanceID{ReplicaID: 0, InstanceNum: 3},
		pb.InstanceID{ReplicaID: 1, InstanceNum: 1},
		pb.InstanceID{ReplicaID: 1, InstanceNum: 2},
	}
	assertMaxDeps()

	// Crete a new command for replica 2 and verify the new max deps.
	newCmd.Span.EndKey = pb.Key("d")
	p.changeID(t, 2)
	p.onRequest(newCmd)
	expMaxDeps[2] = []pb.InstanceID{
		pb.InstanceID{ReplicaID: 0, InstanceNum: 3},
		pb.InstanceID{ReplicaID: 1, InstanceNum: 3},
	}
	assertMaxDeps()
}
