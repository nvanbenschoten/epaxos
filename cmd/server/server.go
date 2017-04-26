package main

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/nvanbenschoten/epaxos/epaxos"
	epaxospb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
	"github.com/nvanbenschoten/epaxos/transport"
	transpb "github.com/nvanbenschoten/epaxos/transport/transportpb"
)

const (
	// tickInterval is the interval at which the Paxos state machine "ticks".
	tickInterval = 10 * time.Millisecond
)

type server struct {
	id     epaxospb.ReplicaID
	node   epaxos.Node
	logger epaxos.Logger
	ticker *time.Ticker

	server          *transport.EPaxosServer
	clients         map[epaxospb.ReplicaID]*transport.EPaxosClient
	unavailClients  map[epaxospb.ReplicaID]struct{}
	pendingRequests map[uuid.UUID]chan<- transpb.KVResult

	keyValueMap map[string][]byte
}

func newServer(ph parsedHostfile) (*server, error) {
	// Create a new EPaxosServer to listen on.
	ps, err := transport.NewEPaxosServer(ph.myPort)
	if err != nil {
		return nil, err
	}

	// Create EPaxosClients for each other host in the network.
	clients := make(map[epaxospb.ReplicaID]*transport.EPaxosClient, len(ph.peerAddrs))
	for _, addr := range ph.peerAddrs {
		pc, err := transport.NewEPaxosClient(addr.AddrStr())
		if err != nil {
			return nil, err
		}
		clients[epaxospb.ReplicaID(addr.Idx)] = pc
	}

	config := ph.toPaxosConfig()
	return &server{
		id:              config.ID,
		node:            epaxos.StartNode(config),
		logger:          config.Logger,
		ticker:          time.NewTicker(tickInterval),
		server:          ps,
		clients:         clients,
		unavailClients:  make(map[epaxospb.ReplicaID]struct{}, len(ph.peerAddrs)),
		pendingRequests: make(map[uuid.UUID]chan<- transpb.KVResult),
		keyValueMap:     make(map[string][]byte),
	}, nil
}

func (s *server) Stop() {
	s.ticker.Stop()
	for _, c := range s.clients {
		c.Close()
	}
	s.server.Stop()
	s.node.Stop()
}

func (s *server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-s.ticker.C:
				s.node.Tick()
			case m := <-s.server.Msgs():
				s.node.Step(ctx, *m)
			case req := <-s.server.Requests():
				s.registerClientRequest(req)
				s.node.Propose(ctx, req.Command)
			case rd := <-s.node.Ready():
				// saveToStorage(rd.PersistentState)

				if err := s.sendAll(ctx, rd.Messages); err != nil {
					s.logger.Warning(err)
				}

				s.handleExecutedCmds(rd.ExecutedCommands)
			case <-ctx.Done():
				return
			}
		}
	}()
	defer s.Stop()
	return s.server.Serve()
}

func (s *server) registerClientRequest(req transport.Request) {
	s.pendingRequests[req.Command.ID] = req.ReturnC
}

func (s *server) handleExecutedCmds(committed []epaxospb.Command) {
	for _, cmd := range committed {
		// This is where we would perform some deterministic update to the
		// server's state machine.
		s.logger.Infof("Executed command %+v", cmd)
		res := s.executeCommand(cmd)

		if ret, ok := s.pendingRequests[cmd.ID]; ok {
			delete(s.pendingRequests, cmd.ID)
			ret <- res
			close(ret)
		}
	}
}

func (s *server) executeCommand(cmd epaxospb.Command) transpb.KVResult {
	if cmd.Span.EndKey != nil {
		s.logger.Panicf("unexpected EndKey in command %+v", cmd)
	}
	key := string(cmd.Span.Key)
	if cmd.Writing {
		s.keyValueMap[key] = cmd.Data
	}
	value := s.keyValueMap[key]
	return transpb.KVResult{
		Key:   cmd.Span.Key,
		Value: value,
	}
}

func (s *server) sendAll(ctx context.Context, msgs []epaxospb.Message) error {
	outboxes := make(map[epaxospb.ReplicaID][]epaxospb.Message)
	for _, m := range msgs {
		outboxes[m.To] = append(outboxes[m.To], m)
	}
	for to, toMsgs := range outboxes {
		if _, unavail := s.unavailClients[to]; unavail {
			continue
		}
		if err := s.sendAllTo(ctx, toMsgs, to); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) sendAllTo(
	ctx context.Context, msgs []epaxospb.Message, to epaxospb.ReplicaID,
) (err error) {
	c, ok := s.clients[to]
	if !ok {
		return errors.Errorf("message found with unknown destination: %v", to)
	}
	defer func() {
		if grpc.Code(err) == codes.Unavailable {
			// If the node is down, record that it's unavailable so that we
			// dont continue to sent to it.
			s.logger.Warningf("detected node %d unavailable", to)
			s.unavailClients[to] = struct{}{}
			c.Close()
		}
	}()
	stream, err := c.DeliverMessage(ctx)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if m.To != to {
			panic("unexpected destination")
		}
		if err := stream.Send(&m); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}
