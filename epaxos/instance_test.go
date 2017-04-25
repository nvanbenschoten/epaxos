package epaxos

import (
	"sort"
	"testing"

	"reflect"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

func (p *epaxos) assertOutbox(t *testing.T, outbox ...pb.Message) {
	if a, e := p.msgs, outbox; !reflect.DeepEqual(a, e) {
		t.Errorf("expected outbox %+v, found %+v", e, a)
	}
}

func (p *epaxos) assertOutboxEmpty(t *testing.T) {
	p.assertOutbox(t)
}

var (
	testingCmd          = makeTestingCommand("a", "z")
	testingInstanceMeta = pb.InstanceMeta{
		Replica:     0,
		InstanceNum: 3,
	}
	testingInstanceState = pb.InstanceState{
		Command: &testingCmd,
		SeqNum:  6,
		Deps: []pb.Dependency{
			{ReplicaID: 0, InstanceNum: 1},
			{ReplicaID: 0, InstanceNum: 2},
			{ReplicaID: 1, InstanceNum: 1},
			{ReplicaID: 1, InstanceNum: 2},
			{ReplicaID: 2, InstanceNum: 1},
		},
	}
)

func TestTransitionToPreAccept(t *testing.T) {
	p := newTestingEPaxos()
	p.assertOutboxEmpty(t)

	// Create a new command and transition to PreAccept.
	newInst := p.onRequest(testingCmd)

	// Assert state.
	newInst.assertState(preAccepted)

	// Assert outbox.
	msg := pb.Message{
		InstanceMeta: testingInstanceMeta,
		Type:         pb.WrapMessageInner(&pb.PreAccept{InstanceState: testingInstanceState}),
	}
	p.assertOutbox(t, msg.WithDestination(1), msg.WithDestination(2))
}

func preAcceptMsg() (pb.InstanceMeta, pb.InstanceState, pb.Message) {
	instMeta := pb.InstanceMeta{Replica: 1, InstanceNum: 3}
	instState := testingInstanceState
	msg := pb.Message{
		InstanceMeta: instMeta,
		Type:         pb.WrapMessageInner(&pb.PreAccept{InstanceState: instState}),
	}
	return instMeta, instState, msg
}

// TestOnPreAcceptWithNoNewInfo tests how a replica behaves when it receives
// a PreAccept message and it has no other information to add. It should not
// matter if the local replica has extra commands if they do not interfere.
// It should return a PreAcceptOK message.
func TestOnPreAcceptWithNoNewInfo(t *testing.T) {
	for _, extraCmd := range []bool{false, true} {
		p := newTestingEPaxos()
		p.assertOutboxEmpty(t)

		if extraCmd {
			// Add a command with a larger sequence number. In this scenerio, Replica 1
			// is not aware of this command, which is why it its proposed sequence
			// number did not take this command into account.
			inst03 := p.newInstance(0, 3)
			inst03.cmd = makeTestingCommand("zz", "zzz")
			inst03.seq = 6
			inst03.deps = map[pb.Dependency]struct{}{}
			p.commands[0].ReplaceOrInsert(inst03)
		}

		instMeta, instState, msg := preAcceptMsg()
		p.Step(msg)

		// Verify internal instance state after receiving message.
		maxInst := p.maxInstance(1)
		if a, e := maxInst.i, instMeta.InstanceNum; a != e {
			t.Errorf("expected new instance with instance num %v, found %v", e, a)
		}
		if a, e := maxInst.seq, pb.SeqNum(6); a != e {
			t.Errorf("expected new instance with seq num %v, found %v", e, a)
		}
		if a, e := maxInst.depSlice(), instState.Deps; !reflect.DeepEqual(a, e) {
			t.Errorf("expected new instance with deps %+v, found %+v", e, a)
		}

		// Verify outbox after receiving message.
		reply := pb.Message{
			To:           1,
			InstanceMeta: instMeta,
			Type:         pb.WrapMessageInner(&pb.PreAcceptOK{}),
		}
		p.assertOutbox(t, reply)
	}
}

// TestOnPreAcceptWithExtraInterferingCommand tests how a replica behaves when it
// receives a PreAccept message and it find that the command should be given additional
// dependencies and a larger sequence number. It should return a PreAcceptReply message
// with the extra dependencies.
func TestOnPreAcceptWithExtraInterferingCommand(t *testing.T) {
	p := newTestingEPaxos()
	p.assertOutboxEmpty(t)

	// Add a command with a larger sequence number. In this scenerio, Replica 1
	// is not aware of this command, which is why it its proposed sequence
	// number did not take this command into account.
	inst03 := p.newInstance(0, 3)
	inst03.cmd = makeTestingCommand("a", "z")
	inst03.seq = 6
	inst03.deps = map[pb.Dependency]struct{}{}
	p.commands[0].ReplaceOrInsert(inst03)

	instMeta, instState, msg := preAcceptMsg()
	p.Step(msg)

	// Verify internal instance state after receiving message.
	maxInst := p.maxInstance(1)
	if a, e := maxInst.i, instMeta.InstanceNum; a != e {
		t.Errorf("expected new instance with instance num %v, found %v", e, a)
	}
	if a, e := maxInst.seq, pb.SeqNum(7); a != e {
		t.Errorf("expected new instance with seq num %v, found %v", e, a)
	}

	// The extra command should be part of the deps.
	expDeps := append(instState.Deps, pb.Dependency{
		ReplicaID:   0,
		InstanceNum: 3,
	})
	sort.Sort(pb.Dependencies(expDeps))
	if a, e := maxInst.depSlice(), expDeps; !reflect.DeepEqual(a, e) {
		t.Errorf("expected new instance with deps %+v, found %+v", e, a)
	}

	// Verify outbox after receiving message.
	reply := pb.Message{
		To:           1,
		InstanceMeta: instMeta,
		Type: pb.WrapMessageInner(&pb.PreAcceptReply{
			UpdatedSeqNum: 7,
			UpdatedDeps:   expDeps,
		}),
	}
	p.assertOutbox(t, reply)
}

func TestOnPreAcceptOK(t *testing.T) {
	p := newTestingEPaxos()

	newInst := p.onRequest(testingCmd)
	p.clearMsgs()

	assertPreAcceptReplies := func(e int) {
		if a := newInst.preAcceptReplies; a != e {
			t.Errorf("expected %d preAcceptReplies, found %d", e, a)
		}
	}

	// Assert state.
	newInst.assertState(preAccepted)
	assertPreAcceptReplies(0)

	// Send PreAcceptOK.
	p.Step(pb.Message{
		To:           0,
		InstanceMeta: testingInstanceMeta,
		Type:         pb.WrapMessageInner(&pb.PreAcceptOK{}),
	})

	// Assert state.
	newInst.assertState(committed)
	assertPreAcceptReplies(1)

	// Assert outbox.
	msg := pb.Message{
		InstanceMeta: testingInstanceMeta,
		Type:         pb.WrapMessageInner(&pb.Commit{InstanceState: testingInstanceState}),
	}
	p.assertOutbox(t, msg.WithDestination(1), msg.WithDestination(2))
}

func TestOnPreAcceptReply(t *testing.T) {
	p := newTestingEPaxos()

	newInst := p.onRequest(testingCmd)
	p.clearMsgs()

	assertPreAcceptReplies := func(e int) {
		if a := newInst.preAcceptReplies; a != e {
			t.Errorf("expected %d preAcceptReplies, found %d", e, a)
		}
	}

	// Assert state.
	newInst.assertState(preAccepted)
	assertPreAcceptReplies(0)

	// Send PreAcceptOK.
	updatedDeps := append([]pb.Dependency(nil), testingInstanceState.Deps...)
	updatedDeps = append(updatedDeps, pb.Dependency{
		ReplicaID:   2,
		InstanceNum: 2,
	})
	p.Step(pb.Message{
		To:           0,
		InstanceMeta: testingInstanceMeta,
		Type: pb.WrapMessageInner(&pb.PreAcceptReply{
			UpdatedSeqNum: 7,
			UpdatedDeps:   updatedDeps,
		}),
	})

	// Assert state.
	newInst.assertState(accepted)
	assertPreAcceptReplies(1)

	// Assert outbox.
	instanceState := testingInstanceState
	instanceState.SeqNum = 7
	instanceState.Deps = updatedDeps
	msg := pb.Message{
		InstanceMeta: testingInstanceMeta,
		Type:         pb.WrapMessageInner(&pb.Accept{InstanceState: instanceState}),
	}
	p.assertOutbox(t, msg.WithDestination(1), msg.WithDestination(2))
}
