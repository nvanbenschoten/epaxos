package transport

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	epaxospb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
	transpb "github.com/nvanbenschoten/epaxos/transport/transportpb"
)

func init() {
	// Silence gRPC logging.
	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
}

// Request represents a request to perform a client update. It includes
// a channel to return the globally ordered result on.
type Request struct {
	Command epaxospb.Command
	ReturnC chan<- transpb.KVResult
}

// EPaxosServer handles internal and external RPC messages for an EPaxos node.
type EPaxosServer struct {
	msgC chan *epaxospb.Message
	reqC chan Request

	lis        net.Listener
	grpcServer *grpc.Server
}

// NewEPaxosServer creates a new EPaxosServer.
func NewEPaxosServer(port int) (*EPaxosServer, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	ps := &EPaxosServer{
		msgC:       make(chan *epaxospb.Message, 16),
		reqC:       make(chan Request, 16),
		lis:        lis,
		grpcServer: grpc.NewServer(),
	}
	transpb.RegisterEPaxosTransportServer(ps.grpcServer, ps)
	transpb.RegisterKVServiceServer(ps.grpcServer, ps)
	return ps, nil
}

// DeliverMessage implements the PaxosTransportServer interface. It receives
// each message from the client stream and passes it to the server's message
// channel.
func (ps *EPaxosServer) DeliverMessage(
	stream transpb.EPaxosTransport_DeliverMessageServer,
) error {
	ctx := stream.Context()
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&transpb.Empty{})
			}
			return err
		}
		select {
		case ps.msgC <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Read implements the KVServiceServer interface. It receives the KVReadRequest
// from the client and passes it as a Request on the server's update channel.
// The method will block until the update is globally ordered.
func (ps *EPaxosServer) Read(
	ctx context.Context, req *transpb.KVReadRequest,
) (*transpb.KVResult, error) {
	cmd := epaxospb.Command{
		ID: rand.Uint64(),
		Span: epaxospb.Span{
			Key: epaxospb.Key(req.Key),
		},
		Writing: false,
	}
	ret := make(chan transpb.KVResult, 1)
	ps.reqC <- Request{
		Command: cmd,
		ReturnC: ret,
	}
	select {
	case res := <-ret:
		return &res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write implements the KVServiceServer interface. It receives the KVWriteRequest
// from the client and passes it as a Request on the server's update channel.
// The method will block until the update is globally ordered and applied.
func (ps *EPaxosServer) Write(
	ctx context.Context, req *transpb.KVWriteRequest,
) (*transpb.KVResult, error) {
	cmd := epaxospb.Command{
		ID: rand.Uint64(),
		Span: epaxospb.Span{
			Key: epaxospb.Key(req.Key),
		},
		Writing: true,
		Data:    req.Value,
	}
	ret := make(chan transpb.KVResult, 1)
	ps.reqC <- Request{
		Command: cmd,
		ReturnC: ret,
	}
	select {
	case res := <-ret:
		return &res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Msgs returns the channel that all Paxos messages will be delivered from
// the server on.
func (ps *EPaxosServer) Msgs() <-chan *epaxospb.Message {
	return ps.msgC
}

// Requests returns the channel that all client requests will be delivered from
// the server on.
func (ps *EPaxosServer) Requests() <-chan Request {
	return ps.reqC
}

// Serve begins serving on server, blocking until Stop is called or an error
// is observed.
func (ps *EPaxosServer) Serve() error {
	return ps.grpcServer.Serve(ps.lis)
}

// Stop stops the EPaxosServer.
func (ps *EPaxosServer) Stop() {
	close(ps.msgC)
	ps.grpcServer.Stop()
}
