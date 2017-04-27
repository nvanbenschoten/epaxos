package transport

import (
	"time"

	"google.golang.org/grpc"

	transpb "github.com/nvanbenschoten/epaxos/transport/transportpb"
)

var clientOpts = []grpc.DialOption{
	grpc.WithInsecure(),
	grpc.WithBlock(),
	grpc.WithTimeout(15 * time.Second),
}

// EPaxosClient is a client stub implementing the EPaxosTransportClient
// interface.
type EPaxosClient struct {
	transpb.EPaxosTransportClient
	*grpc.ClientConn
}

// NewEPaxosClient creates a new EPaxosClient.
func NewEPaxosClient(addr string) (*EPaxosClient, error) {
	conn, err := grpc.Dial(addr, clientOpts...)
	if err != nil {
		return nil, err
	}
	client := transpb.NewEPaxosTransportClient(conn)
	return &EPaxosClient{client, conn}, nil
}
