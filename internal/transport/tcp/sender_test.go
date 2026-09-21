package tcp

import (
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

	sender, err := Dial(listener.Addr().String())
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

	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}

	actual := buffer[:n]

	if string(actual) != string(expected) {
		t.Fatalf("got %q, want %q", actual, expected)
	}
}
