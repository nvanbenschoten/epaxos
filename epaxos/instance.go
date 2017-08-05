package epaxos

import (
	"fmt"
	"sort"

	"github.com/google/btree"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

type instance struct {
	p  *epaxos
	is pb.InstanceState

	// command-leader state
	preAcceptReplies int
	differentReplies bool
	slowPathTimer    tickingTimer
	acceptReplies    int
}

const slowPathTimout = 2

func (p *epaxos) newInstance(r pb.ReplicaID, i pb.InstanceNum) *instance {
	inst := &instance{
		p: p,
		is: pb.InstanceState{
			InstanceID: pb.InstanceID{
				ReplicaID:   r,
				InstanceNum: i,
			},
		},
	}
	inst.slowPathTimer = makeTickingTimer(slowPathTimout, func() {
		inst.transitionTo(pb.InstanceState_Accepted)
	})
	return inst
}

func (p *epaxos) newInstanceFromState(is *pb.InstanceState) *instance {
	inst := &instance{p: p, is: *is}
	inst.slowPathTimer = makeTickingTimer(slowPathTimout, func() {
		inst.transitionTo(pb.InstanceState_Accepted)
	})
	return inst
}

//
// BTree Functions
//

// Less implements the btree.Item interface.
func (inst *instance) Less(than btree.Item) bool {
	return inst.is.Less(&than.(*instance).is)
}

// instanceKey creates a key to index into the commands btree.
func instanceKey(i pb.InstanceNum) btree.Item {
	return &instance{is: pb.InstanceState{InstanceID: pb.InstanceID{InstanceNum: i}}}
}

//
// Persistence Funcions
//

func (inst *instance) persist() {
	inst.p.storage.PersistInstance(&inst.is)
}

//
// executable Functions
//

func (inst *instance) Identifier() executableID {
	return pb.InstanceID{
		ReplicaID:   inst.is.ReplicaID,
		InstanceNum: inst.is.InstanceNum,
	}
}

func (inst *instance) Dependencies() []executableID {
	deps := make([]executableID, 0, len(inst.is.Deps))
	for _, dep := range inst.is.Deps {
		deps = append(deps, dep)
	}
	return deps
}

// ExecutesBefore determines which of two instances execute first. The ordering
// is based on sequence numbers (lamport logical clocks), which break ties in
// strongly connected components. If the sequence numbers are also the same,
// then we break ties based on ReplicaID, because commands in the same SCC will
// always be from different replicas.
func (inst *instance) ExecutesBefore(b executable) bool {
	instB := b.(*instance)
	if seqA, seqB := inst.is.SeqNum, instB.is.SeqNum; seqA != seqB {
		return seqA < seqB
	}
	return inst.is.ReplicaID < instB.is.ReplicaID
}

func (inst *instance) Execute() {
	inst.transitionTo(pb.InstanceState_Executed)
}

//
// State-Transitions
//

type stateTransition struct {
	from, to pb.InstanceState_Status
}

func (st stateTransition) String() string {
	return fmt.Sprintf("{%v -> %v}", st.from, st.to)
}

var stateTransitions = map[stateTransition]func(*instance){
	stateTransition{pb.InstanceState_None, pb.InstanceState_PreAccepted}: func(inst *instance) {
		inst.broadcastPreAccept()
	},
	stateTransition{pb.InstanceState_PreAccepted, pb.InstanceState_Accepted}: func(inst *instance) {
		inst.broadcastAccept()
	},
	stateTransition{pb.InstanceState_PreAccepted, pb.InstanceState_Committed}: func(inst *instance) {
		inst.broadcastCommit()
		inst.prepareToExecute()
	},
	stateTransition{pb.InstanceState_Accepted, pb.InstanceState_Committed}: func(inst *instance) {
		inst.broadcastCommit()
		inst.prepareToExecute()
	},
	stateTransition{pb.InstanceState_Committed, pb.InstanceState_Executed}: func(inst *instance) {
		inst.p.deliverExecutedCommand(*inst.is.Command)
	},
}

func (inst *instance) transitionTo(to pb.InstanceState_Status) {
	st := stateTransition{from: inst.is.Status, to: to}
	action, ok := stateTransitions[st]
	if !ok {
		inst.p.logger.Panicf("unexpected state transition %s", st)
	}

	inst.is.Status = to
	action(inst)
	inst.persist()
}

func (inst *instance) restartTransition() {
	cur := inst.is.Status
	st := stateTransition{from: cur - 1, to: cur}
	action := stateTransitions[st]
	action(inst)
}

func (inst *instance) isStates(states ...pb.InstanceState_Status) bool {
	cur := inst.is.Status
	for _, s := range states {
		if s == cur {
			return true
		}
	}
	return false
}

func (inst *instance) assertState(valid ...pb.InstanceState_Status) {
	if !inst.isStates(valid...) {
		inst.p.logger.Panicf("unexpected state %v; expected %v", inst.is.Status, valid)
	}
}

// broadcastPreAccept broadcasts a PreAccept message to all other nodes.
func (inst *instance) broadcastPreAccept() {
	inst.broadcast(&pb.PreAccept{InstanceData: inst.instanceData()})
}

// broadcastAccept broadcasts an Accept message to all other nodes.
func (inst *instance) broadcastAccept() {
	inst.broadcast(&pb.Accept{InstanceData: inst.instanceDataWithoutCommand()})
}

// broadcastCommit broadcasts a Commit message to all other nodes.
func (inst *instance) broadcastCommit() {
	inst.broadcast(&pb.Commit{InstanceData: inst.instanceData()})
}

//
// Message Handlers
//

func (inst *instance) onPreAccept(pa *pb.PreAccept) {
	// Only handle if this is a new instance, and set the state to preAccepted.
	if !inst.isStates(pb.InstanceState_None, pb.InstanceState_PreAccepted) {
		inst.p.logger.Debugf("ignoring PreAccept message while in state %v: %v", inst.is.Status, pa)
		return
	}
	inst.is.Status = pb.InstanceState_PreAccepted

	// Determine the local sequence number and deps for this command.
	maxLocalSeq, localDeps := inst.p.seqAndDepsForCommand(pa.Command, inst.is.InstanceID)

	// Record the command for the instance.
	inst.is.Command = pa.Command

	// The updated sequence number is set to the maximum of the local maximum
	// sequence number and the the PreAccept's sequence number
	inst.is.SeqNum = pb.MaxSeqNum(pa.SeqNum, maxLocalSeq+1)

	// Determine the union of the local dependencies and the PreAccept's dependencies.
	depsUnion := localDeps
	for _, dep := range pa.Deps {
		depsUnion[dep] = struct{}{}
	}
	inst.is.Deps = depSliceFromMap(depsUnion)

	// If the sequence number and the deps turn out to be the same as those in
	// the PreAccept message, reply with a simple PreAcceptOK message.
	if inst.is.SeqNum == pa.SeqNum && len(inst.is.Deps) == len(pa.Deps) {
		inst.reply(&pb.PreAcceptOK{})
		return
	}

	// Reply to PreAccept message with updated information.
	inst.reply(&pb.PreAcceptReply{
		UpdatedSeqNum: inst.is.SeqNum,
		UpdatedDeps:   inst.is.Deps,
	})
}

// fastPathAvailable returns whether the fast path is still available, given
// (possibly zero) more PreAcceptReply messages.
func (inst *instance) fastPathAvailable() bool {
	return !inst.differentReplies
}

func (inst *instance) onPreAcceptOK(paOK *pb.PreAcceptOK) {
	if !inst.isStates(pb.InstanceState_PreAccepted) {
		inst.p.logger.Debugf("ignoring PreAcceptOK message while in state %v: %v", inst.is.Status, paOK)
		return
	}

	inst.preAcceptReplies++
	inst.onEitherPreAcceptReply()
}

func (inst *instance) onPreAcceptReply(paReply *pb.PreAcceptReply) {
	if !inst.isStates(pb.InstanceState_PreAccepted) {
		inst.p.logger.Debugf("ignoring PreAcceptReply message while in state %v: %v", inst.is.Status, paReply)
		return
	}

	// Check whether this PreAccept reply is identical to our preAccept or if
	// the remote peer returned extra information that we weren't aware of. An
	// identical fast path quorum allows us to skip the Paxos-Accept phase.
	newSeq := paReply.UpdatedSeqNum
	if newSeq > inst.is.SeqNum {
		inst.is.SeqNum = newSeq
		inst.differentReplies = true
	}

	// Merge remote deps into local deps.
	oldDepsLen := len(inst.is.Deps)
	inst.is.Deps = unionDepSlices(inst.is.Deps, paReply.UpdatedDeps)
	if oldDepsLen != len(inst.is.Deps) {
		inst.differentReplies = true
	}

	inst.preAcceptReplies++
	inst.onEitherPreAcceptReply()
}

func (inst *instance) onEitherPreAcceptReply() {
	replies := inst.preAcceptReplies + 1 // +1 for leader
	takeFastPath := !inst.differentReplies && inst.p.fastQuorum(replies)
	takeSlowPath := inst.p.quorum(replies)
	switch {
	case takeFastPath:
		inst.p.unregisterTimer(&inst.slowPathTimer)
		inst.transitionTo(pb.InstanceState_Committed)
	case takeSlowPath:
		// We have enough replies to take the slow path, however we don't want to
		// take it immediately in-case it's possible to take the fast path instead.
		if !inst.fastPathAvailable() {
			// Since the fast path will never be available, take the slow path.
			inst.p.unregisterTimer(&inst.slowPathTimer)
			inst.transitionTo(pb.InstanceState_Accepted)
		} else if !inst.slowPathTimer.isSet() {
			// Delay for a few ticks before taking slow path to allow for the fast
			// path quorum to be achieved.
			inst.p.registerOneTimeTimer(&inst.slowPathTimer)
		} else {
			// Timer already set. This reply will help us get to the fast path.
		}
	}
}

func (inst *instance) onAccept(a *pb.Accept) {
	if !inst.isStates(pb.InstanceState_None, pb.InstanceState_PreAccepted, pb.InstanceState_Accepted) {
		inst.p.logger.Debugf("ignoring Accept message while in state %v: %v", inst.is.Status, a)
		return
	}

	inst.is.Status = pb.InstanceState_Accepted
	inst.replaceInstanceData(a.SeqNum, a.Deps)
	inst.reply(&pb.AcceptOK{})
}

func (inst *instance) onAcceptOK(aOK *pb.AcceptOK) {
	if !inst.isStates(pb.InstanceState_Accepted) {
		inst.p.logger.Debugf("ignoring AcceptOK message while in state %v: %v", inst.is.Status, aOK)
		return
	}

	inst.acceptReplies++
	if inst.p.quorum(inst.acceptReplies + 1 /* +1 for leader */) {
		inst.transitionTo(pb.InstanceState_Committed)
	}
}

func (inst *instance) onCommit(c *pb.Commit) {
	if !inst.isStates(pb.InstanceState_None, pb.InstanceState_PreAccepted, pb.InstanceState_Accepted) {
		inst.p.logger.Debugf("ignoring Commit message while in state %v: %v", inst.is.Status, c)
		return
	}

	inst.is.Status = pb.InstanceState_Committed
	inst.is.Command = c.Command
	inst.replaceInstanceData(c.SeqNum, c.Deps)
	inst.prepareToExecute()
}

//
// Utility Functions
//

func (inst *instance) instanceDataWithoutCommand() pb.InstanceData {
	return pb.InstanceData{
		SeqNum: inst.is.SeqNum,
		Deps:   inst.is.Deps,
	}
}

func (inst *instance) instanceData() pb.InstanceData {
	is := inst.instanceDataWithoutCommand()
	is.Command = inst.is.Command
	return is
}

func (inst *instance) replaceInstanceData(newSeq pb.SeqNum, newDeps []pb.InstanceID) {
	inst.is.SeqNum = newSeq
	inst.is.Deps = newDeps
}

func depSliceFromMap(depsMap map[pb.InstanceID]struct{}) []pb.InstanceID {
	deps := make([]pb.InstanceID, 0, len(depsMap))
	for dep := range depsMap {
		deps = append(deps, dep)
	}
	// Sort so that the order is deterministic.
	sort.Sort(pb.InstanceIDs(deps))
	return deps
}

func unionDepSlices(a, b []pb.InstanceID) []pb.InstanceID {
	depUnion := make(map[pb.InstanceID]struct{})
	for _, dep := range a {
		depUnion[dep] = struct{}{}
	}
	for _, dep := range b {
		depUnion[dep] = struct{}{}
	}
	return depSliceFromMap(depUnion)
}

func (inst *instance) prepareToExecute() {
	inst.p.prepareToExecute(inst)
}
