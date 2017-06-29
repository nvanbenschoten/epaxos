package epaxos

import (
	"github.com/gogo/protobuf/proto"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

func (p *epaxos) sendTo(m proto.Message, to pb.ReplicaID, inst *instance) {
	mm := pb.WrapMessage(m)
	mm.To = to
	mm.InstanceID = inst.is.InstanceID
	// mm.Ballot = 1 TODO
	p.msgs = append(p.msgs, mm)
}

func (p *epaxos) broadcast(m proto.Message, inst *instance) {
	for _, node := range p.nodes {
		if node != p.id {
			p.sendTo(m, node, inst)
		}
	}
}

func (inst *instance) reply(m proto.Message) {
	inst.p.sendTo(m, inst.is.ReplicaID, inst)
}

func (inst *instance) broadcast(m proto.Message) {
	inst.p.broadcast(m, inst)
}

func (p *epaxos) clearMsgs() {
	p.msgs = nil
}
