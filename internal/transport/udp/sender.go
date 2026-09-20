package udp

import (
	"fmt"
	"net"
)

/*
Responsible for opening udp socket and managing the
open connection.

Handles writes out to other socket
*/

// Sender hanldes the connected UDP client socket
type Sender struct {
	conn *net.UDPConn
}

// Dial resolves the target ip addr and opens a UDP socket bounded
// to an emphemeral port whose target is the IP address passed
func Dial(target string) (*Sender, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, fmt.Errorf("resolve the UDP target %q: %w", target, err)
	}

	// Allocate a connected UDP socket connected to target addr and
	// assign it emphemeral port
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial UDP target %q: %w", target, err)
	}

	return &Sender{
		conn: conn,
	}, nil
}

// Send writes the a byte payload to the open UDP socket and OS handles
// transmitting that information out to target address of the network
func (s *Sender) Send(payload []byte) error {
	n, err := s.conn.Write(payload)
	if err != nil {
		return fmt.Errorf("send UDP paylaod: %w", err)
	}

	payload_n := len(payload)
	if n != payload_n {
		return fmt.Errorf(
			"short UDP write: wrote %d out of %d bytes",
			n,
			payload_n,
		)
	}

	return nil
}

// Close ends the UDP socket lifecycle
func (s *Sender) Close() error {
	return s.conn.Close()
}
