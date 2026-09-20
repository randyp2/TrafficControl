package udp

import (
	"net"
	"testing"
)

func TestSenderSend(t *testing.T) {
	// Choose any port
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Listen for UDP packets on specified address
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	sender, err := Dial(listener.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	expected := []byte("hello")
	if err := sender.Send(expected); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1024)
	n, _, err := listener.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}

	actual := buffer[:n]
	if string(actual) != string(expected) {
		t.Fatalf("ACTUAL: %q | GOT: %q", actual, expected)
	}
}
