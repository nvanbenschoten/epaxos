package epaxospb

import (
	"github.com/google/btree"
)

// Less implements the btree.Item interface.
func (ihs *InstanceState) Less(than btree.Item) bool {
	return ihs.InstanceNum < than.(*InstanceState).InstanceNum
}
