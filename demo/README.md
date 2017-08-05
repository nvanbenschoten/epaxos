# Distributed Key-Value Store

This is a sample application using the epaxos library.

The application is a distributed key-value store which provides
strongly consistent, high availability read and write access. Each
replica in the system runs on top of [badger](https://github.com/dgraph-io/badger),
a fast, embedded key-value store. It then uses EPaxos to maintain
consistency between each of these replicas.

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
