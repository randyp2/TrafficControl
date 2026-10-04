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

## Performance

The current local UDP baseline delivered every reported send at targets through 150,000 packets per second. Across five runs per target, median pacing error was 0.0006% at 100k and 0.0092% at 150k, with 100% observed delivery.

See [docs/performance.md](docs/performance.md) for the benchmark method, results, and optimization plan.
