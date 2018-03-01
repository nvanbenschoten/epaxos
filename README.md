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
Simple](https://www.microsoft.com/en-us/research/publication/paxos-made-simple/).

Egalitarian Paxos is an efficient, leaderless variation of this protocol,
proposed by Iulian Moraru in his 2013 paper, [There Is More Consensus in
Egalitarian
Parliaments](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf).
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
- Persistence
- Failure Recovery

Features not yet implemented:

- Explicit Prepare Phase
- Optimized Egalitarian Paxos (smaller fast path quorum)
- Membership changes
- Batched commands
- Thrifty operation (see paper)
- [Quorum leases](https://www.cs.cmu.edu/~dga/papers/leases-socc2014.pdf)
- Snapshots


## Building

Run `make` or `make test` to run all tests against the library 

Run `make clean` to clean all build artifacts

Run `make check` to perform linting and static analysis


## Testing

The project comes with an automated test suite which contains both direct unit
tests to test pieces of functionality within the EPaxos state machine, and larger
network tests that test a network of EPaxos nodes. The unit tests are scattered
throughout the `epaxos/*_test.go` files, while the network tests are located in
the `epaxos/epaxos_test.go` file.

To run all tests, run the command `make test`


## Library Interface

The library is designed around the the `epaxos` type, which is a single-threaded
state machine implementing the Egalitarian Paxos consensus protocol. The state
machine can be interacted with only through a `Node` instance, which is a
thread-safe handle to a `epaxos` state machine.

Because the library pushes tasks like storage handling and network transport up
to the users of the library, these users have a few responsibilities. In a loop,
the user should read from the `Node.Ready` channel and process the updates it
contains. These `Ready` struct will contain any updates to the persistent state
of the node that should be synced to disk, and messages that need to be
delivered to other nodes, and any commands that have been successfully committed
and that are ready to be executed. The user should also periodically call
`Node.Tick` in regular interval (probably via a `time.Ticker`).

Together, the state machine handling loop will look something like:

```
for {
    select {
    case <-ticker.C:
        node.Tick()
    case rd := <-node.Ready():
        for _, msg := range rd.Messages {
            send(msg)
        }
        for _, cmd := range rd.ExecutableCommands {
            execute(cmd)
        }
    case <-ctx.Done():
        return
    }
}
```

To propose a change to the state machine, first construct a `pb.Command`
message. The `pb.Command` message contains both an arbitrary byte slice to hold
client updates and additional metadata fields to related to command
interference. Use of these metadata fields in `pb.Command` is the mechanism in
which clients of the library express application-specific command interference
semantics. `pb.Commands` operate in a virtual keyspace, and each command
operates over a subset of this keyspace, which is expressed in the `Span` field.
`pb.Commands` can also be specified as reads or writes using the `Writing`
field. Interference between commands is then defined as two commands whose
`Spans` overlap, where at-least one of the commands is `Writing`.

After a `pb.Command` is constructed with the desired update and the necessary
interference constrains, call:

```
node.Propose(ctx, command)
```

Once executable, the `pb.Command` will appear in the `rd.ExecutableCommands` slice.



