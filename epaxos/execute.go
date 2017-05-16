package epaxos

import (
	"sort"
)

// executableID is a unique value that identifies an executable. The value
// needs to be hashable and comparable.
type executableID interface{}

// executable is a value that is executable by the executor. It contains a
// set of dependencies that need to be executed before
type executable interface {
	// Identifier returns a uniquely identifying executableID.
	Identifier() executableID
	// Dependencies returns the executableIDs of all executables this
	// executable depends on. These dependencies must be executed before
	// the executable if they topologically sort into an earlier strongly
	// connected component.
	Dependencies() []executableID
	// ExecutesBefore determines if the receiver should execute before the
	// parameter executable in cases where both depend on each other (ie. they
	// are part of a strongly connected component in the topologically sorted
	// dependency graph.
	ExecutesBefore(executable) bool
	// Execute executes the executable. It should only be called once.
	Execute()
}

// history can answer the question of whether an executable has already been
// executed or not.
type history interface {
	// HasExecuted determines if the executable identified by the provided ID
	// has already executed.
	HasExecuted(executableID) bool
}

// executor is responsible for executing executables in-order, based on their
// dependencies. On each run of the executor, the executor topologically sorts
// all executables and executes those that are able to be executed.
type executor struct {
	h history

	// values scoped to a single run of the executor's tarjan's strongly
	// connected components algorithm.
	vertices   map[executableID]*tarjanNode
	index      int
	stack      []*tarjanNode
	components []scc
}

func makeExecutor(h history) executor {
	return executor{
		h:        h,
		vertices: make(map[executableID]*tarjanNode),
	}
}

func (e *executor) run() {
	// Separate the strongly connected components while reverse topologically
	// sorting.
	comps := e.strongConnect()

	// Execute each strongly connected component, in-order.
	for _, comp := range comps {
		e.executeSCC(comp)
	}

	// Reset the executor.
	e.reset()
}

func (e *executor) addExecs(execs ...executable) {
	for _, exec := range execs {
		e.addExec(exec)
	}
}

func (e *executor) addExec(exec executable) {
	e.vertices[exec.Identifier()] = &tarjanNode{exec: exec}
}

func (e *executor) reset() {
	e.index = 0
	e.stack = e.stack[:0]
	e.components = e.components[:0]
}

type tarjanNode struct {
	exec executable

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

// strongConnect runs the Tarjan's strongly connected components algorithm,
// returning an ordered slice of strongly connected components.
func (e *executor) strongConnect() []scc {
	for _, v := range e.vertices {
		v.reset()

		for _, dep := range v.exec.Dependencies() {
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
		var component scc
		for w := (*tarjanNode)(nil); w != v; {
			w = e.pop()
			w.onStack = false
			component = append(component, w)
		}
		e.components = append(e.components, component)
	}
}

func (e *executor) executeSCC(comp scc) {
	for _, v := range comp {
		for _, dep := range v.exec.Dependencies() {
			// The dependency should either be in this strongly connected
			// component or have already been executed (possibly by an earlier
			// SCC). If those conditions are not true, abort executing the
			// entire SCC.
			if w, ok := e.vertices[dep]; ok && comp.contains(w) {
				// The dependency is in this SCC.
				continue
			}
			if !e.h.HasExecuted(dep) {
				// The dependency is not in this SCC and has not been executed
				// in a prerequisite SCC. We cannot execute at this time.
				return
			}
		}
	}

	// Sort the component based on SCC execution order.
	sort.Slice(comp, func(i, j int) bool {
		return comp[i].exec.ExecutesBefore(comp[j].exec)
	})

	// Execute each executable in the SCC, in-order.
	for _, v := range comp {
		e.execute(v.exec)
	}
}

func (e *executor) execute(exec executable) {
	delete(e.vertices, exec.Identifier())
	exec.Execute()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
