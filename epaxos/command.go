package epaxos

import (
	"github.com/google/btree"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

func (p *epaxos) localCommands() *btree.BTree {
	return p.commands[p.id]
}

func (p *epaxos) maxInstance(r pb.ReplicaID) *instance {
	if maxInstItem := p.commands[r].Max(); maxInstItem != nil {
		return maxInstItem.(*instance)
	}
	return nil
}

func (p *epaxos) maxInstanceNum(r pb.ReplicaID) pb.InstanceNum {
	if maxInst := p.maxInstance(r); maxInst != nil {
		return maxInst.i
	}
	return 0
}

func (p *epaxos) maxSeqNum(r pb.ReplicaID) pb.SeqNum {
	if maxInst := p.maxInstance(r); maxInst != nil {
		return maxInst.seq
	}
	return 0
}

func (p *epaxos) maxDeps(r pb.ReplicaID) map[pb.Dependency]struct{} {
	if maxInst := p.maxInstance(r); maxInst != nil {
		return maxInst.deps
	}
	return nil
}

func (p *epaxos) hasExecuted(dep pb.Dependency) bool {
	if instItem := p.commands[dep.ReplicaID].Get(instanceKey(dep.InstanceNum)); instItem != nil {
		return instItem.(*instance).state == executed
	}
	p.logger.Panicf("unknown dependency %v", dep)
	return false
}

// seqAndDepsForCommand determines the locally known maximum interfering sequence
// number and dependencies for a given command.
func (p *epaxos) seqAndDepsForCommand(cmd pb.Command) (pb.SeqNum, map[pb.Dependency]struct{}) {
	maxSeq := pb.SeqNum(0)
	deps := make(map[pb.Dependency]struct{})
	for rID, cmds := range p.commands {
		cmds.Ascend(func(i btree.Item) bool {
			if inst := i.(*instance); inst.cmd.Interferes(cmd) {
				// TODO optimize!
				dep := pb.Dependency{ReplicaID: rID, InstanceNum: inst.i}
				deps[dep] = struct{}{}
				maxSeq = pb.MaxSeqNum(maxSeq, inst.seq)
			}
			return true
		})
	}
	return maxSeq, deps
}

func (p *epaxos) onRequest(cmd pb.Command) *instance {
	// Determine the smallest unused instance number.
	i := p.maxInstanceNum(p.id) + 1

	// Add a new instance for the command in the local commands.
	maxLocalSeq, localDeps := p.seqAndDepsForCommand(cmd)
	newInst := p.newInstance(p.id, i)
	newInst.cmd = cmd
	newInst.seq = maxLocalSeq + 1
	newInst.deps = localDeps
	p.localCommands().ReplaceOrInsert(newInst)

	// Transition the new instance into a preAccepted state.
	newInst.transitionToPreAccept()
	return newInst
}

func (p *epaxos) prepareToExecute(inst *instance) {
	inst.assertState(committed)
	p.executor.enqueueCommitted(inst)
	p.executor.run()
}

func (p *epaxos) execute(inst *instance) {
	inst.assertState(committed)
	inst.state = executed
	// TODO GC commands
	p.deliverExecutedCommand(inst.cmd)
}
