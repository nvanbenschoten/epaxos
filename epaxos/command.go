package epaxos

import (
	"github.com/cockroachdb/cockroach/pkg/util/interval"
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
	return p.maxTruncatedInstanceNum[r]
}

func (p *epaxos) maxSeqNum(r pb.ReplicaID) pb.SeqNum {
	if maxInst := p.maxInstance(r); maxInst != nil {
		return maxInst.seq
	}
	return p.maxTruncatedSeqNum
}

func (p *epaxos) maxDeps(r pb.ReplicaID) map[pb.Dependency]struct{} {
	if maxInst := p.maxInstance(r); maxInst != nil {
		return maxInst.deps
	}
	return nil
}

func (p *epaxos) getInstance(r pb.ReplicaID, i pb.InstanceNum) *instance {
	if instItem := p.commands[r].Get(instanceKey(i)); instItem != nil {
		return instItem.(*instance)
	}
	return nil
}

func (p *epaxos) hasTruncated(r pb.ReplicaID, i pb.InstanceNum) bool {
	return i <= p.maxTruncatedInstanceNum[r]
}

func (p *epaxos) hasExecuted(r pb.ReplicaID, i pb.InstanceNum) bool {
	if p.hasTruncated(r, i) {
		return true
	}
	if inst := p.getInstance(r, i); inst != nil {
		return inst.state == executed
	}
	return false
}

func (p *epaxos) hasCommitted(r pb.ReplicaID, i pb.InstanceNum) bool {
	if p.hasExecuted(r, i) {
		return true
	}
	if inst := p.getInstance(r, i); inst != nil {
		return inst.state == committed
	}
	return false
}

// HasExecuted implements the history interface.
func (p *epaxos) HasExecuted(e executableID) bool {
	d := e.(pb.Dependency)
	return p.hasExecuted(d.ReplicaID, d.InstanceNum)
}

// seqAndDepsForCommand determines the locally known maximum interfering sequence
// number and dependencies for a given command.
func (p *epaxos) seqAndDepsForCommand(cmd pb.Command) (pb.SeqNum, map[pb.Dependency]struct{}) {
	maxSeq := p.maxTruncatedSeqNum
	deps := make(map[pb.Dependency]struct{})

	cmdRage := rangeForCmd(cmd)
	for rID, cmds := range p.commands {
		// Adding to the writeRG and readRG allows us to minimize the number of
		// dependencies we add for this command without building a directed graph
		// and topological sorting. This relies on the interference relation for
		// commands ove a given key-range being transitive. It also relies on the
		// causality of subsequent instances within the same replica instance space.
		// The logic here is very similar to that in CockroachDB's Command Queue.
		cmds.Descend(func(i btree.Item) bool {
			inst := i.(*instance)
			addDep := func() {
				dep := pb.Dependency{ReplicaID: rID, InstanceNum: inst.i}
				deps[dep] = struct{}{}
			}

			if otherCmd := inst.cmd; otherCmd.Interferes(cmd) {
				maxSeq = pb.MaxSeqNum(maxSeq, inst.seq)

				otherCmdRange := rangeForCmd(otherCmd)
				if otherCmd.Writing {
					// We add the other command's range to the RangeGroup and
					// observe if it grows the group. If it does, that means
					// that it is not a full transitive dependency of other
					// dependencies of ours. If it is, that means that we do
					// not need to depend on it because previous dependencies
					// necessarily already have it as a dependency themself.
					if p.rangeGroup.Add(otherCmdRange) {
						addDep()
						if p.rangeGroup.Len() == 1 && p.rangeGroup.Encloses(cmdRage) {
							return false
						}
					}
				} else {
					// We check if the current RangeGroup overlaps the read
					// dependency. Reads don't depend on reads, so this will
					// only happen if a write was inserted that fully covers
					// the read.
					if !p.rangeGroup.Overlaps(otherCmdRange) {
						addDep()
					}
				}
			}
			return true
		})
		p.rangeGroup.Clear()
	}
	return maxSeq, deps
}

func rangeForCmd(cmd pb.Command) interval.Range {
	startKey := cmd.Span.Key
	endKey := cmd.Span.EndKey
	if len(endKey) == 0 {
		endKey = append(startKey, 0)
	}
	return interval.Range{
		Start: interval.Comparable(startKey),
		End:   interval.Comparable(endKey),
	}
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
	p.executor.addExec(inst)
	// TODO pull executor into a different goroutine and run asynchronously.
	p.executor.run()
	p.truncateCommands()
}

func (p *epaxos) execute(inst *instance) {
	inst.assertState(committed)
	inst.state = executed
	p.deliverExecutedCommand(inst.cmd)
}

func (p *epaxos) truncateCommands() {
	for r, cmds := range p.commands {
		var executedItems []btree.Item
		cmds.Ascend(func(i btree.Item) bool {
			if i.(*instance).state == executed {
				executedItems = append(executedItems, i)
				return true
			}
			return false
		})
		if len(executedItems) > 0 {
			curMaxInstNum := p.maxTruncatedInstanceNum[r]
			for _, executedItem := range executedItems {
				inst := executedItem.(*instance)
				p.maxTruncatedSeqNum = pb.MaxSeqNum(p.maxTruncatedSeqNum, inst.seq)
				curMaxInstNum = pb.MaxInstanceNum(curMaxInstNum, inst.i)
				cmds.Delete(executedItem)
			}
			p.maxTruncatedInstanceNum[r] = curMaxInstNum
		}
	}
}
