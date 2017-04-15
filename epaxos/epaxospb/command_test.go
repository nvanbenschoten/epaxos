package epaxospb

import (
	"testing"
)

func TestKeyEqual(t *testing.T) {
	a1 := Key("a1")
	a2 := Key("a2")
	if !a1.Equal(a1) {
		t.Errorf("expected keys equal")
	}
	if a1.Equal(a2) {
		t.Errorf("expected different keys not equal")
	}
}

func TestKeyCompare(t *testing.T) {
	testCases := []struct {
		a, b    Key
		compare int
	}{
		{nil, nil, 0},
		{nil, Key("\x00"), -1},
		{Key("\x00"), Key("\x00"), 0},
		{Key(""), Key("\x00"), -1},
		{Key("a"), Key("b"), -1},
		{Key("a\x00"), Key("a"), 1},
		{Key("a\x00"), Key("a\x01"), -1},
	}
	for i, c := range testCases {
		if c.a.Compare(c.b) != c.compare {
			t.Fatalf("%d: unexpected %q.Compare(%q): %d", i, c.a, c.b, c.compare)
		}
	}
}

func TestSpanOverlaps(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}

	testData := []struct {
		s1, s2   Span
		overlaps bool
	}{
		{sA, sA, true},
		{sA, sD, false},
		{sA, sBtoD, false},
		{sBtoD, sA, false},
		{sD, sBtoD, false},
		{sBtoD, sD, false},
		{sA, sAtoC, true},
		{sAtoC, sA, true},
		{sAtoC, sAtoC, true},
		{sAtoC, sBtoD, true},
		{sBtoD, sAtoC, true},
	}
	for i, test := range testData {
		for _, swap := range []bool{false, true} {
			s1, s2 := test.s1, test.s2
			if swap {
				s1, s2 = s2, s1
			}
			if o := s1.Overlaps(s2); o != test.overlaps {
				t.Errorf("%d: expected overlap %t; got %t between %s vs. %s", i, test.overlaps, o, s1, s2)
			}
		}
	}
}

func TestCommandInterferes(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}

	rA := Command{Writing: false, Span: sA}
	wA := Command{Writing: true, Span: sA}
	rD := Command{Writing: false, Span: sD}
	wD := Command{Writing: true, Span: sD}
	rAtoC := Command{Writing: false, Span: sAtoC}
	wAtoC := Command{Writing: true, Span: sAtoC}
	rBtoD := Command{Writing: false, Span: sBtoD}
	wBtoD := Command{Writing: true, Span: sBtoD}

	testData := []struct {
		c1, c2     Command
		interferes bool
	}{
		{rA, rA, false},
		{rA, wA, true},
		{rA, rD, false},
		{rA, wD, false},
		{rA, rBtoD, false},
		{rA, wBtoD, false},
		{rA, rAtoC, false},
		{rA, wAtoC, true},
		{wA, rA, true},
		{wA, wA, true},
		{wA, rD, false},
		{wA, wD, false},
		{wA, rBtoD, false},
		{wA, wBtoD, false},
		{wA, rAtoC, true},
		{wA, wAtoC, true},
	}
	for i, test := range testData {
		for _, swap := range []bool{false, true} {
			c1, c2 := test.c1, test.c2
			if swap {
				c1, c2 = c2, c1
			}
			if inter := c1.Interferes(c2); inter != test.interferes {
				t.Errorf("%d: expected interfere %t; got %t between %s vs. %s", i, test.interferes, inter, c1, c2)
			}
		}
	}
}
