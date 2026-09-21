package tcp

import (
	"fmt"
	"net"
)

type Sender struct {
	conn *net.TCPConn
}

// Dial takes the target string and opens up a TCP socket
func Dial(target string) (*Sender, error) {
	addr, err := net.ResolveTCPAddr("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("resolve TCP target %q: %w", target, err)
	}

	// Initiate 3-way handshake (syn -> syn-ack -> ack) to remote addr
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial TCP target %q: %w", target, err)
	}

	return &Sender{
		conn: conn,
	}, nil
}

// Send writes raw byte payload to the remote connection
func (s *Sender) Send(payload []byte) error {
	n, err := s.conn.Write(payload)
	if err != nil {
		return fmt.Errorf("send TCP payload: %w", err)
	}

	if n != len(payload) {
		return fmt.Errorf(
			"short TCP write: wrote %d of %d bytes",
			n,
			len(payload),
		)
	}

	return nil
}

// Close resolves/closes the TCP socket connection that was opened
func (s *Sender) Close() error {
	return s.conn.Close()
}
