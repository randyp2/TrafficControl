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

## Performance

The current local UDP baseline delivered every reported send at targets through 200,000 packets per second. Across five runs per target, median pacing error stayed at or below 0.0007% at 100k, 150k, and 200k, with 100% observed delivery.

See [docs/performance.md](docs/performance.md) for the benchmark method, results, and optimization plan.
