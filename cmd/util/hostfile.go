package util

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Addr represents an indexed network address.
type Addr struct {
	Idx  int
	Host string
	Port int
}

// AddrStr returns a string representation of the network address.
func (a Addr) AddrStr() string {
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

// ParseHostfile parses the hostfile file, returning a list of indexed
// addresses.
func ParseHostfile(filename string, defaultPort int) ([]Addr, error) {
	var addrs []Addr

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()
		host, port, err := parseHost(line, defaultPort)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, Addr{
			Idx:  i,
			Host: host,
			Port: port,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return addrs, nil
}

func parseHost(line string, defaultPort int) (string, int, error) {
	s := strings.Split(line, ":")
	switch len(s) {
	case 1:
		return s[0], defaultPort, nil
	case 2:
		p, err := strconv.Atoi(s[1])
		return s[0], p, err
	default:
		return "", 0, errors.Errorf("could not parse line %q", line)
	}
}
