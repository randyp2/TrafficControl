package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestSenderSend(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	sender, err := Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	conn, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	expected := []byte("hello dude")

	if err := sender.Send(expected); err != nil {
		t.Fatal(err)
	}

	// Set readdeadline timeout for the connection
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1024)

	n, err := io.ReadFull(conn, buffer[:len(expected)])
	if err != nil {
		t.Fatal(err)
	}

	actual := buffer[:n]

	if string(actual) != string(expected) {
		t.Fatalf("got %q, want %q", actual, expected)
	}
}

func TestDialHonorsCanceledContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sender, err := Dial(ctx, listener.Addr().String())
	if sender != nil {
		_ = sender.Close()
		t.Fatal("Dial returned a connection after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial error = %v, want context.Canceled", err)
	}
}

func TestSenderTimesOutWhenPeerStopsReading(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	sender, err := Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	peer, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := sender.conn.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}

	// Keep the peer idle and exceed the small socket buffers to force a blocked write.
	done := make(chan error, 1)
	go func() {
		done <- sender.Send(make([]byte, 1024*1024))
	}()
	select {
	case err := <-done:
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("Send error = %v, want a write timeout", err)
		}
	case <-time.After(writeTimeout + time.Second):
		t.Fatal("Send stayed blocked beyond the write timeout")
	}
}
