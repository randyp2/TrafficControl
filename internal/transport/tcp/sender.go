package tcp

import (
	"context"
	"fmt"
	"net"
	"time"
)

const (
	connectTimeout = 5 * time.Second
	writeTimeout   = 2 * time.Second
)

// Sender writes generated payloads over one TCP connection.
type Sender struct {
	conn *net.TCPConn
}

// Dial takes the target string and opens up a TCP socket.
// Resolution and connection establishment respect ctx and a five-second timeout.
func Dial(ctx context.Context, target string) (*Sender, error) {
	dialer := net.Dialer{
		Timeout: connectTimeout,
	}

	// Initiate 3-way handshake (syn -> syn-ack -> ack) to remote addr
	conn, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, fmt.Errorf("dial TCP target %q: %w", target, err)
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("expected TCP connection, got %T", conn)
	}

	return &Sender{
		conn: tcpConn,
	}, nil
}

// Send writes raw byte payload to the remote connection. This informs us
// whether the data was sent NOT RECIEVED
// Each write has a two-second deadline; errors may follow a partial write.
func (s *Sender) Send(payload []byte) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("set TCP write deadline: %w", err)
	}

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
