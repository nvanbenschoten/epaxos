# Egalitarian Paxos

_A pluggable implementation of the Egalitarian Paxos Consensus Protocol_

Paxos is a protocol for solving consensus through state machine replication in
an asynchronous environment with unreliable processes. It can tolerate up to F
concurrent replica failures with 2F+1 total replicas. This consensus protocol is
then extended with a stable leader optimization to a replication protocol
(commonly referred to as Multi-Paxos) to assign global, persistent, total order
to a sequence of client updates. The protocol works by having multiple replicas
work in parallel to maintain the same state. This state is updated on each
request from a client by each replica, allowing it to be automatically
replicated and preserved even in the case of failures. The basic algorithm was
famously described by Leslie Lamport in his 1998 paper, [The Part-Time
Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/).
It was later clarified in his follow-up paper from 2001, [Paxos Made
Simple](https://www.microsoft.com/en-us/research/publication/paxos-made-simple/?from=https%3A%2F%2Fresearch.microsoft.com%2Fen-us%2Fum%2Fpeople%2Flamport%2Fpubs%2Fpaxos-simple.pdf).

Egalitarian Paxos is an efficient, leaderless variation of this protocol,
proposed by Iulian Moraru in his 2013 paper, [There Is More Consensus in
Egalitarian
Parliaments](http://delivery.acm.org/10.1145/2520000/2517350/p358-moraru.pdf?ip=209.6.79.135&id=2517350&acc=OA&key=4D4702B0C3E38B35%2E4D4702B0C3E38B35%2E4D4702B0C3E38B35%2EC42B82B87617960C&CFID=755096137&CFTOKEN=25359323&__acm__=1493150278_439ca70486fb3a890d145fe85ea9f1c5).
It provides strong consistency with optimal wide-area latency, perfect
load-balancing across replicas (both in the local and the wide area), and
constant availability for up to F failures. Concretely, it provides the
following properties:

- High throughput, low latency
- Constant availability
- Load distributed evenly across all replicas (no leader)
- Limited by fastest replicas, not slowest
- Can always use closest replicas (low latency)
- 1 round-trip fast path

It does so by breaking the global command slot space into subspaces, each owned
by a single replica. Replicas then attach ordering constraints to each command
while voting on them to allow for proper ordering during command execution. For
more intuition on how this works, check out the [presentation given at SOSP
'13](https://www.youtube.com/watch?v=KxoWlUZNKn8), and for a full technical
report and proof of correctness of the protocol, check out [A Proof of
Correctness for Egalitarian
Paxos](http://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-13-111.pdf).

This library is implemented with a minimalistic philosophy. The main `epaxos`
package implements only the core EPaxos algorithm, with storage handling,
network transport, and physical clocks left to the clients of the library. This
minimalism buys flexibility, determinism, and performance. The design was
heavily inspired by [CoreOS's raft
library](https://github.com/coreos/etcd/tree/master/raft).


## Features

The epaxos implementation is a full implementation of the Egalitarian Paxos
replication protocol. Features include:

- Command replication
- Command compaction

Features not yet implemented:

- Recovery
- Persistence
- Membership changes
- Batched commands
- Thrifty operation (see paper)
- [Quorum leases](https://www.cs.cmu.edu/~dga/papers/leases-socc2014.pdf)
- Snapshots


## Building

Run `make` to build the binaries `client` and `server`

Run `make clean` to clean all build artifacts

Run `make check` to perform linting and static analysis


## Running

The project comes with two sample binaries, `client` and `server`. These
binaries exemplify the proper use of the library to create a distributed
key-value store.

To run a server process, a command like the following can be used:

```
./server -p 54321 -h hostfile
```

All server processes are identical; there is no designated leader process. There
is also no order in which processes need to be brought up, although they will
exit after 15 seconds if a connection cannot be established any of their peers.

To run a client process, a command like the following can be used:

```
./client -p 54321 -h hostfile
```

The client will connect to all available hosts in the hostfile and then prompt
the user for an update string. Whenever an update is entered, it will send the
update to a random available server, which attempts to globally order the update.

### Verbose Mode (server only)

Adding the `-v` (`--verbose`) flag will turn on verbose mode, which will
print logging information to standard error. This information includes details
about all messages sent and received, as well as round timeout information.

### Command Line Arguments

A full list of command line arguments for the two binaries can be seen by
passing the `--help` flag.


## Testing

The project comes with an automated test suite which contains both direct unit
tests to test pieces of functionality within the EPaxos state machine, and larger
network tests that test a network of EPaxos nodes. The unit tests are scattered
throughout the `epaxos/*_test.go` files, while the network tests are located in
the `epaxos/epaxos_test.go` file.

To run all tests, run the command `make test`
