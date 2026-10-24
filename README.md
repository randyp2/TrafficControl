# TrafficControl

TrafficControl is a config-driven network traffic generator for exercising systems before real clients and production traffic are available. Scenarios can run multiple streams and schedule rate changes, pauses, resumes, and stops.

## Quick start

Start a UDP listener:

```bash
go run ./cmd/trafficcontrol listen --protocol udp --address 127.0.0.1:5000
```

Run a scenario in another terminal:

```bash
go run ./cmd/trafficcontrol run examples/scenarios/benchmark/udp/50k.yaml
```

Output shows per-stream generation and sending rates, intentional drops, and a final summary. Colors are enabled for terminal output; use `NO_COLOR=1` to disable them. Redirected output stays plain text.

Scheduler diagnostics are hidden by default. Put `--verbose` before the scenario path to enable them on stderr:

```bash
go run ./cmd/trafficcontrol run --verbose examples/scenarios/loss/toggle.yaml
```

In the rate table, `LOSS %` is the current configured probability. The summary's drop percentage covers the entire run. Generation and sending averages include paused time and are not measures of per-packet timing accuracy.

## TCP

Start a TCP listener, then run the example in another terminal:

```bash
go run ./cmd/trafficcontrol listen --protocol tcp --address 127.0.0.1:6000
go run ./cmd/trafficcontrol run examples/scenarios/basic/tcp.yaml
```

The example stops after five seconds. Stop the listener with Ctrl-C to see its final totals. Compare sender bytes with receiver bytes: TCP is a byte stream, so receiver read counts cannot measure payload or packet counts. Receiver metrics show bytes/s, total bytes, total accepted connections, active connections, and read errors. Normal disconnects are not errors, and one failed connection does not stop the listener.

TCP connection attempts have a five-second timeout, and each write has a two-second deadline. Ctrl-C and scheduled stop events interrupt blocked writes by closing the connection. An actual write timeout fails the stream; a partial payload may already have reached the peer. An immediate stop can discard delayed payloads and does not drain the receiver.

Rate, latency, pause/resume, and loss decisions run before TCP writes. Loss skips generated application payloads; it does not simulate lost TCP segments, retransmissions, or congestion control.

## Performance

The current local UDP baseline delivered every reported send at targets through 200,000 packets per second. Across five runs per target, median pacing error stayed at or below 0.0007% at 100k, 150k, and 200k, with 100% observed delivery.

See [docs/performance.md](docs/performance.md) for the benchmark method, results, and optimization plan.
