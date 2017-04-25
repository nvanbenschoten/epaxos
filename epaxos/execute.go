package epaxos

import (
	"sort"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

type executor struct {
	p *epaxos

	vertices   map[pb.Dependency]*tarjanNode
	index      int
	stack      []*tarjanNode
	components []scc
}

func makeExecutor(p *epaxos) executor {
	return executor{
		p:        p,
		vertices: make(map[pb.Dependency]*tarjanNode),
	}
}

func depForInstance(inst *instance) pb.Dependency {
	return pb.Dependency{
		ReplicaID:   inst.r,
		InstanceNum: inst.i,
	}
}

func (e *executor) enqueueCommitted(inst *instance) {
	dep := depForInstance(inst)
	e.vertices[dep] = &tarjanNode{inst: inst}
}

func (e *executor) run() {
	if len(e.vertices) == 0 {
		return
	}

	comps := e.strongConnect()
	for _, comp := range comps {
		e.executeSCC(comp)
	}
}

func (e *executor) strongConnect() []scc {
	e.index = 0
	e.stack = e.stack[:0]
	e.components = e.components[:0]

	for _, v := range e.vertices {
		v.reset()

		for dep := range v.inst.deps {
			if w, ok := e.vertices[dep]; ok {
				v.depNodes = append(v.depNodes, w)
			}
		}
	}

	for _, v := range e.vertices {
		if !v.visited() {
			e.visit(v)
		}
	}

	return e.components
}

func (e *executor) push(n *tarjanNode) {
	e.stack = append(e.stack, n)
}

func (e *executor) pop() *tarjanNode {
	l := len(e.stack) - 1
	n := e.stack[l]
	e.stack = e.stack[:l]
	return n
}

func (e *executor) visit(v *tarjanNode) {
	v.index = e.index
	v.lowlink = e.index
	e.index++

	v.onStack = true
	e.push(v)

	for _, w := range v.depNodes {
		if !w.visited() {
			e.visit(w)
			v.lowlink = min(v.lowlink, w.lowlink)
		} else if w.onStack {
			v.lowlink = min(v.lowlink, w.index)
		}
	}

	if v.lowlink == v.index {
		var component []*tarjanNode
		for w := (*tarjanNode)(nil); w != v; {
			w = e.pop()
			w.onStack = false
			component = append(component, w)
		}
		e.components = append(e.components, component)
	}
}

type tarjanNode struct {
	inst *instance

	// depNodes holds all dependencies of the instance that were also
	// tarjanNodes in the current graph.
	depNodes []*tarjanNode

	index   int
	lowlink int
	onStack bool
}

func (v *tarjanNode) reset() {
	v.depNodes = v.depNodes[:0]
	v.index = -1
	v.lowlink = -1
	v.onStack = false
}

func (v *tarjanNode) visited() bool {
	return v.index >= 0
}

type scc []*tarjanNode

func (comp scc) contains(v *tarjanNode) bool {
	for _, w := range comp {
		if w == v {
			return true
		}
	}
	return false
}

func (e *executor) executeSCC(comp scc) {
	for _, v := range comp {
		for dep := range v.inst.deps {
			// The dependency should either be in this strongly connected
			// component or have already been executed (possibly by an earlier
			// SCC). If those conditions are not true, abort executing the entire
			// SCC.
			if w, ok := e.vertices[dep]; ok && comp.contains(w) {
				continue
			}
			if !e.p.hasExecuted(dep) {
				return
			}
		}
	}

	// Sort the component based on sequence numbers (lamport logical clocks),
	// which break ties in strongly connected components.
	sort.Slice(comp, func(i, j int) bool {
		return comp[i].inst.seq < comp[j].inst.seq
	})

	for _, v := range comp {
		delete(e.vertices, depForInstance(v.inst))
		e.p.execute(v.inst)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
