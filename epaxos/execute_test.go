package epaxos

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"reflect"
)

type edge struct {
	from int
	to   int
}

func (e edge) String() string {
	return fmt.Sprintf("%d->%d", e.from, e.to)
}

func graphString(edges []edge) string {
	var b bytes.Buffer
	b.WriteRune('[')
	for i, e := range edges {
		if i != 0 {
			b.WriteRune(',')
		}
		b.WriteString(e.String())
	}
	b.WriteRune(']')
	return b.String()
}

type execNode struct {
	id        int
	deps      []int
	onExecute func(int)
}

// execNode implements the executable interface.
func (n execNode) Identifier() executableID { return n.id }
func (n execNode) Dependencies() []executableID {
	ids := make([]executableID, len(n.deps))
	for i, dep := range n.deps {
		ids[i] = dep
	}
	return ids
}
func (n execNode) ExecutesBefore(b executable) bool { return n.id < b.(execNode).id }
func (n execNode) Execute() {
	if n.onExecute != nil {
		n.onExecute(n.id)
	}
}

func execNodesString(nodes []execNode) string {
	var b bytes.Buffer
	b.WriteRune('[')
	for i, n := range nodes {
		if i != 0 {
			b.WriteRune(',')
		}
		fmt.Fprint(&b, n)
	}
	b.WriteRune(']')
	return b.String()
}

// emptyHistory is an implementation of history that does not contain any
// previously executed executables.
type emptyHistory struct{}

func (emptyHistory) HasExecuted(executableID) bool { return false }

// historySet is an implementation of history that contains a set of all
// executed executables.
type historySet map[executableID]struct{}

func (hs historySet) HasExecuted(e executableID) bool { _, ok := hs[e]; return ok }
func (hs historySet) SetExecuted(e executableID)      { hs[e] = struct{}{} }

func TestFindStronglyConnectedComponents(t *testing.T) {
	testCases := []struct {
		edges      []edge
		components [][]int
	}{
		{
			edges: []edge{
				{1, 0},
				{0, 2},
				{2, 1},
				{0, 3},
				{3, 4},
			},
			components: [][]int{
				{4}, {3}, {0, 1, 2},
			},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 3},
			},
			components: [][]int{
				{3}, {2}, {1}, {0},
			},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 0},
				{1, 3},
				{1, 4},
				{1, 6},
				{3, 5},
				{4, 5},
				{6, 4},
				{4, 3},
			},
			components: [][]int{
				{5}, {3}, {4}, {6}, {0, 1, 2},
			},
		},
		{
			edges: []edge{
				{0, 1},
				{0, 3},
				{1, 2},
				{1, 4},
				{2, 0},
				{2, 6},
				{3, 2},
				{4, 5},
				{4, 6},
				{5, 6},
				{5, 7},
				{5, 8},
				{5, 9},
				{6, 4},
				{7, 9},
				{8, 9},
				{9, 8},
			},
			components: [][]int{
				{8, 9}, {7}, {4, 5, 6}, {0, 1, 2, 3},
			},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 3},
				{2, 4},
				{3, 0},
				{4, 2},
			},
			components: [][]int{
				{0, 1, 2, 3, 4},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(graphString(tC.edges), func(t *testing.T) {
			e := makeExecutor(&emptyHistory{})

			// Construct execNodes from test edges.
			nodes := make(map[int]*execNode)
			for _, ed := range tC.edges {
				maybeMakeNode := func(i int) {
					if nodes[i] == nil {
						nodes[i] = &execNode{id: i}
					}
				}
				maybeMakeNode(ed.from)
				maybeMakeNode(ed.to)
				node := nodes[ed.from]
				node.deps = append(node.deps, ed.to)
			}

			// Add execNodes to executor and strongConnect.
			for _, node := range nodes {
				e.addExecs(node)
			}
			compsNodes := e.strongConnect()

			// Convert SCC to ints and compare.
			comps := make([][]int, len(compsNodes))
			for i, compNodes := range compsNodes {
				comp := make([]int, len(compNodes))
				for j, v := range compNodes {
					comp[j] = v.exec.(*execNode).id
				}
				sort.Ints(comp)
				comps[i] = comp
			}
			if a, e := comps, tC.components; !reflect.DeepEqual(a, e) {
				t.Errorf("expected strongly connected components %v, found %v", e, a)
			}
		})
	}
}

func TestExecuteStronglyConnectedComponent(t *testing.T) {
	initialExecuted := []int{1, 3}
	testCases := []struct {
		scc       []execNode
		execution []int
	}{
		// Single node with no dependencies; execute.
		{
			scc: []execNode{
				{id: 4, deps: []int{}},
			},
			execution: []int{4},
		},
		// Single node with executed dependencies; execute.
		{
			scc: []execNode{
				{id: 4, deps: []int{1, 3}},
			},
			execution: []int{4},
		},
		// Single node with non-executed dependencies; don't execute.
		{
			scc: []execNode{
				{id: 4, deps: []int{1, 2, 3}},
			},
			execution: nil,
		},
		// Multiple node with no dependencies; execute in-order.
		{
			scc: []execNode{
				{id: 4, deps: []int{9}},
				{id: 9, deps: []int{5}},
				{id: 5, deps: []int{8}},
				{id: 8, deps: []int{4}},
			},
			execution: []int{4, 5, 8, 9},
		},
		// Multiple node with executed dependencies; execute in-order.
		{
			scc: []execNode{
				{id: 4, deps: []int{9}},
				{id: 9, deps: []int{1, 5}},
				{id: 5, deps: []int{3, 8}},
				{id: 8, deps: []int{1, 4}},
			},
			execution: []int{4, 5, 8, 9},
		},
		// Multiple node with non-executed dependencies; don't execute any.
		{
			scc: []execNode{
				{id: 4, deps: []int{2, 9}},
				{id: 9, deps: []int{1, 5}},
				{id: 5, deps: []int{3, 8}},
				{id: 8, deps: []int{1, 4}},
			},
			execution: nil,
		},
	}
	for _, tC := range testCases {
		desc := fmt.Sprintf("%s->%v", execNodesString(tC.scc), tC.execution)
		t.Run(desc, func(t *testing.T) {
			// Create the initial history and the executor.
			h := make(historySet)
			for _, e := range initialExecuted {
				h.SetExecuted(e)
			}
			e := makeExecutor(h)

			// Keep track of each executed id.
			var executed []int
			onExecute := func(id int) {
				// Update the history.
				h.SetExecuted(id)
				// Track executed ids.
				executed = append(executed, id)
			}

			// Add each node to the executor.
			for _, node := range tC.scc {
				node.onExecute = onExecute
				e.addExec(node)
			}

			// Create the strongly connected component.
			comps := e.strongConnect()
			if l := len(comps); l != 1 {
				t.Fatalf("expected single SCC, found %d", l)
			}

			// Execute the SCC and verify correct execution order.
			e.executeSCC(comps[0])
			if a, e := executed, tC.execution; !reflect.DeepEqual(a, e) {
				t.Errorf("expected execution order %+v, found %+v", e, a)
			}
		})
	}
}
