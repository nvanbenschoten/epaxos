package epaxos

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"reflect"

	pb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
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

func TestStronglyConnectedComponents(t *testing.T) {
	for _, test := range []struct {
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
	} {
		t.Run(graphString(test.edges), func(t *testing.T) {
			e := makeExecutor(nil)

			instances := make(map[int]*instance)
			for _, e := range test.edges {
				maybeMakeInstance := func(i int) {
					if instances[i] == nil {
						instances[i] = &instance{
							i:    pb.InstanceNum(i),
							deps: make(map[pb.Dependency]struct{}),
						}
					}
				}
				maybeMakeInstance(e.from)
				maybeMakeInstance(e.to)
				dep := pb.Dependency{
					InstanceNum: pb.InstanceNum(e.to),
				}
				instances[e.from].deps[dep] = struct{}{}
			}

			for _, inst := range instances {
				e.enqueueCommitted(inst)
			}

			compsNodes := e.strongConnect()
			comps := make([][]int, len(compsNodes))
			for i, compNodes := range compsNodes {
				comp := make([]int, len(compNodes))
				for j, v := range compNodes {
					comp[j] = int(v.inst.i)
				}
				sort.Ints(comp)
				comps[i] = comp
			}

			if a, e := comps, test.components; !reflect.DeepEqual(a, e) {
				t.Errorf("expected strongly connected components %v, found %v", e, a)
			}
		})
	}
}
