package main

import (
	"context"
	"log"
	"math/rand"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/nvanbenschoten/epaxos/cmd/util"
	"github.com/nvanbenschoten/epaxos/transport"
	transpb "github.com/nvanbenschoten/epaxos/transport/transportpb"
)

type client struct {
	id        uint64
	seqNum    uint64
	serverSet map[*transport.ExternalClient]struct{}
}

func newClient(addrs []util.Addr) (*client, error) {
	serverSet := make(map[*transport.ExternalClient]struct{}, len(addrs))
	for _, addr := range addrs {
		addrStr := addr.AddrStr()
		c, err := transport.NewExternalClient(addrStr)
		if err != nil {
			log.Printf("could not connect to %s", addrStr)
		} else {
			serverSet[c] = struct{}{}
		}
	}
	if len(serverSet) == 0 {
		return nil, errors.Errorf("no servers available")
	}

	return &client{
		id:        rand.Uint64(),
		serverSet: serverSet,
	}, nil
}

func (c *client) sendReadRequest(
	ctx context.Context, key []byte,
) (*transpb.KVResult, error) {
	s := c.randomServer()
	gou, err := s.Read(ctx, &transpb.KVReadRequest{
		Key: key,
	})
	if err != nil {
		return nil, c.onServerError(s, err)
	}
	return gou, nil
}

func (c *client) sendWriteRequest(
	ctx context.Context, key, value []byte,
) (*transpb.KVResult, error) {
	s := c.randomServer()
	gou, err := s.Write(ctx, &transpb.KVWriteRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return nil, c.onServerError(s, err)
	}
	return gou, nil
}

func (c *client) randomServer() *transport.ExternalClient {
	i := rand.Intn(len(c.serverSet))
	for c := range c.serverSet {
		if i == 0 {
			return c
		}
		i--
	}
	panic("unreachable")
}

func (c *client) onServerError(s *transport.ExternalClient, err error) error {
	if grpc.Code(err) == codes.Unavailable {
		// If the node is down, remove it from the server set.
		log.Printf("detected node unavailable")
		delete(c.serverSet, s)
		s.Close()

		// Make sure we still have at least one server available.
		if len(c.serverSet) == 0 {
			log.Fatal("no servers available")
		}
	}
	return err
}
