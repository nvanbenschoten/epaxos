// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kdtree

import (
	"math/rand"
	"sort"
	"testing"

	"gopkg.in/check.v1"
)

type Ints []int

func (a Ints) Len() int                  { return len(a) }
func (a Ints) Less(i, j int) bool        { return a[i] < a[j] }
func (a Ints) Slice(s, e int) SortSlicer { return a[s:e] }
func (a Ints) Swap(i, j int)             { a[i], a[j] = a[j], a[i] }

func (s *S) TestPartition(c *check.C) {
	for p := 0; p < 100; p++ {
		list := make(Ints, 1e5)
		for i := range list {
			list[i] = rand.Int()
		}
		pi := Partition(list, rand.Intn(list.Len()))
		for i := 0; i < pi; i++ {
			c.Check(list[i] <= list[pi], check.Equals, true)
		}
		for i := pi + 1; i < len(list); i++ {
			c.Check(list[i] > list[pi], check.Equals, true)
		}
	}
}

func (s *S) TestPartitionCollision(c *check.C) {
	for p := 0; p < 10; p++ {
		list := make(Ints, 10)
		for i := range list {
			list[i] = rand.Intn(5)
		}
		pi := Partition(list, p)
		for i := 0; i < pi; i++ {
			c.Check(list[i] <= list[pi], check.Equals, true)
		}
		for i := pi + 1; i < len(list); i++ {
			c.Check(list[i] > list[pi], check.Equals, true)
		}
	}
}

func sortSelection(list Ints, k int) int {
	sort.Sort(list)
	return list[k]
}

func (s *S) TestSelect(c *check.C) {
	for k := 0; k < 2121; k++ {
		list := make(Ints, 2121)
		for i := range list {
			list[i] = rand.Intn(1000)
		}
		Select(list, k)
		sorted := append(Ints(nil), list...)
		c.Check(list[k], check.Equals, sortSelection(sorted, k), check.Commentf("\n%d\n%v\n%v", k, list, sorted))
	}
}

func (s *S) TestMedianOfMedians(c *check.C) {
	list := make(Ints, 1e4)
	for i := range list {
		list[i] = rand.Int()
	}
	p := MedianOfMedians(list)
	med := list[p]
	sort.Sort(list)
	var found bool
	for _, v := range list[len(list)*3/10 : len(list)*7/10+1] {
		if v == med {
			found = true
			break
		}
	}
	c.Check(found, check.Equals, true)
}

func (s *S) TestMedianOfRandoms(c *check.C) {
	list := make(Ints, 1e4)
	for i := range list {
		list[i] = rand.Int()
	}
	p := MedianOfRandoms(list, Randoms)
	med := list[p]
	sort.Sort(list)
	var found bool
	for _, v := range list[len(list)*3/10 : len(list)*7/10+1] {
		if v == med {
			found = true
			break
		}
	}
	c.Check(found, check.Equals, true)
}

func BenchmarkMoM(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := make(Ints, 1e4)
		for i := range list {
			list[i] = rand.Int()
		}
		b.StartTimer()
		_ = MedianOfMedians(list)
	}
}

func BenchmarkMoMPartition(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := make(Ints, 1e4)
		for i := range list {
			list[i] = rand.Int()
		}
		b.StartTimer()
		p := MedianOfMedians(list)
		p = Partition(list, p)
	}
}

func BenchmarkRM(b *testing.B) {
	b.StopTimer()
	list := make(Ints, 1e4)
	for i := range list {
		list[i] = rand.Int()
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = MedianOfRandoms(list, list.Len()/1e3)
	}
}

func BenchmarkRMPartition(b *testing.B) {
	b.StopTimer()
	list := make(Ints, 1e4)
	for i := range list {
		list[i] = rand.Int()
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		p := MedianOfRandoms(list, list.Len()/1e3)
		p = Partition(list, p)
	}
}

func BenchmarkSM(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := make(Ints, 1e4)
		for i := range list {
			list[i] = rand.Int()
		}
		b.StartTimer()
		sort.Sort(list)
	}
}
