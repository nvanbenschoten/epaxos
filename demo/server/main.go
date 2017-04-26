package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"

	"github.com/nvanbenschoten/epaxos/demo/util"
	"github.com/nvanbenschoten/epaxos/epaxos"
	"github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
)

const (
	hostfileDesc = "The hostfile is the path to a file that contains " +
		"the list of hostnames that the servers are running on. It should " +
		"be in the format of a hostname per line. The line number indicates " +
		"the identifier of the server, which starts at 0."
	portDesc = "The port identifies on which port each server " +
		"will be listening on for incoming TCP connections from " +
		"clients. It can take any integer from 1024 to 65535."
	idDesc = "The optional id specifier of this process. Only needed if multiple " +
		"processes in the hostfile are running on the same host, otherwise it can " +
		"be deduced from the hostfile. 0-indexed."
	verboseDesc = "Sets the logging level to verbose."
)

var (
	help     = flag.Bool("help", false, "")
	verbose  = flag.BoolP("verbose", "v", false, verboseDesc)
	hostfile = flag.StringP("hostfile", "h", "hostfile", hostfileDesc)
	port     = flag.IntP("port", "p", 2346, portDesc)
	hostID   = flag.IntP("id", "i", -1, idDesc)
)

func main() {
	flag.CommandLine.MarkHidden("help")
	flag.Parse()
	if *help {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, flag.CommandLine.FlagUsagesWrapped(120))
		return
	}

	ph, err := parseHostfile()
	if err != nil {
		log.Fatal(err)
	}

	s, err := newServer(ph)
	if err != nil {
		log.Fatal(err)
	}

	s.Run()
}

func parseHostfile() (parsedHostfile, error) {
	var ph parsedHostfile
	if *hostfile == "" {
		return ph, errors.New("hostfile flag required")
	}

	addrs, err := util.ParseHostfile(*hostfile, *port)
	if err != nil {
		return ph, err
	}

	hostIDSet := *hostID >= 0
	myHostname, err := os.Hostname()
	if err != nil {
		return ph, err
	}

	for _, addr := range addrs {
		if (hostIDSet && addr.Idx == *hostID) || (!hostIDSet && addr.Host == myHostname) {
			if hostIDSet && addr.Host != myHostname {
				return ph, errors.Errorf("id flag %d is not the hostname of this host", *hostID)
			}
			if ph.localInfoSet() {
				return ph, errors.Errorf("local host information set twice in hostfile. " +
					"Consider using the --id flag.")
			}
			ph.myID = addr.Idx
			ph.myPort = addr.Port
		} else {
			ph.peerAddrs = append(ph.peerAddrs, addr)
		}
	}

	if !ph.localInfoSet() {
		if *hostID >= len(ph.peerAddrs) {
			return ph, errors.Errorf("--id flag value too large")
		}
		return ph, errors.Errorf("local hostname %q not in hostfile", myHostname)
	}
	return ph, nil
}

// parsedHostfile represents a hostfile that has been analyzed to determine
// the network settings for the local process's server and the address of
// all remove servers.
type parsedHostfile struct {
	myID      int
	myPort    int
	peerAddrs []util.Addr
}

func (ph parsedHostfile) toPaxosConfig() *epaxos.Config {
	logger := epaxos.NewDefaultLogger()
	if *verbose {
		logger.EnableDebug()
	}
	nodes := make([]epaxospb.ReplicaID, len(ph.peerAddrs)+1)
	for i := range nodes {
		nodes[i] = epaxospb.ReplicaID(i)
	}
	return &epaxos.Config{
		ID:     epaxospb.ReplicaID(ph.myID),
		Nodes:  nodes,
		Logger: logger,
	}
}

func (ph parsedHostfile) localInfoSet() bool {
	return ph.myPort != 0
}
