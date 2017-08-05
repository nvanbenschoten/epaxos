package main

import (
	"encoding/binary"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/nvanbenschoten/epaxos/epaxos"
	"github.com/pkg/errors"

	"github.com/dgraph-io/badger"

	epaxospb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

var (
	dirName = "epaxos-demo"

	nullByte             = byte(0x00)
	userspacePrefix      = []byte("u")
	epaxosPrefix         = []byte("p")
	epaxosHS             = append(epaxosPrefix, []byte("hs")...)
	epaxosInstancePrefix = append(epaxosPrefix, []byte("is")...)
)

type store struct {
	kv *badger.KV
}

func newStore() *store {
	opt := badger.DefaultOptions
	dir, _ := ioutil.TempDir("/tmp", dirName)
	opt.Dir = dir
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)
	return &store{kv: kv}
}

func (s *store) Close() {
	s.kv.Close()
}

func encodeUserKey(key []byte) []byte {
	return append(userspacePrefix, key...)
}

// SetKey sets the given key to the value provided.
func (s *store) SetKey(key, val []byte) {
	s.kv.Set(encodeUserKey(key), val, 0x00)
}

// GetKey gets the value at the given key, or returns false if
// no key exists.
func (s *store) GetKey(key []byte) ([]byte, error) {
	var item badger.KVItem
	if err := s.kv.Get(encodeUserKey(key), &item); err != nil {
		errors.Wrapf(err, "Error while getting key: %q", key)
	}
	return item.Value(), nil
}

// store implements the epaxos.Storage interface.
var _ epaxos.Storage = &store{}

func (s *store) HardState() (hs epaxospb.HardState, found bool) {
	var item badger.KVItem
	if err := s.kv.Get(epaxosHS, &item); err != nil {
		panic(err)
	}
	val := item.Value()
	if val == nil {
		return hs, false
	}
	if err := proto.Unmarshal(val, &hs); err != nil {
		panic(err)
	}
	return hs, true
}

func (s *store) PersistHardState(hs epaxospb.HardState) {
	val, err := proto.Marshal(&hs)
	if err != nil {
		panic(err)
	}
	s.kv.Set(epaxosHS, val, 0x00)
}

func (s *store) Instances() []*epaxospb.InstanceState {
	var insts []*epaxospb.InstanceState

	opt := badger.DefaultIteratorOptions
	itr := s.kv.NewIterator(opt)
	for itr.Rewind(); itr.ValidForPrefix(epaxosInstancePrefix); itr.Next() {
		item := itr.Item()
		val := item.Value()

		inst := &epaxospb.InstanceState{}
		if err := proto.Unmarshal(val, inst); err != nil {
			panic(err)
		}
		insts = append(insts, inst)
	}
	itr.Close()
	return insts
}

func (s *store) PersistInstance(is *epaxospb.InstanceState) {
	val, err := proto.Marshal(is)
	if err != nil {
		panic(err)
	}
	s.kv.Set(encodeInstanceKey(is), val, 0x00)
}

// encodeInstanceKey encodes an InstanceState into a unique key.
//
// encoding scheme:
//   prefix <replicaID> 0x00 <instanceNum>
func encodeInstanceKey(is *epaxospb.InstanceState) []byte {
	l := len(epaxosInstancePrefix) + 2*binary.MaxVarintLen64 + 1
	key := make([]byte, l)
	copy(key, epaxosInstancePrefix)
	end := len(epaxosInstancePrefix)

	end += binary.PutUvarint(key[end:], uint64(is.ReplicaID))

	key[end] = nullByte
	end++

	end += binary.PutUvarint(key[end:], uint64(is.InstanceNum))
	return key[:end]
}
