# Performance

This document records repeatable baselines and the impact of future performance changes.

## Method

- Environment: macOS arm64, Go 1.27.1
- Protocol: UDP over `127.0.0.1`
- Payload: 9 bytes (`benchmark`)
- Duration: 10 seconds
- Sample size: one run per target
- Listener drain time: 250 milliseconds after the sender stopped

## Metrics

- **Target:** The configured packets-per-second rate.
- **Expected:** The target rate multiplied by the configured duration.
- **Sent:** Packets successfully handed to the operating system by the sender.
- **Received:** Datagrams read by the TrafficControl listener.
- **Average sent/s:** Packets sent divided by the sender's actual runtime.
- **Pacing accuracy:** `sent ÷ expected × 100`. This measures how closely the generator followed the requested rate, not network delivery.
- **Delivery:** `received ÷ sent × 100`. This measures how many reported sends reached the listener during the run.

For example, the 100k run sent 991,767 of 1,000,000 expected packets, giving 99.1767% pacing accuracy. The listener received all 991,767 sends, giving 100% observed delivery.

## Baseline

| Target | Expected | Sent | Received | Average sent/s | Pacing accuracy | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000/s | 10,000 | 10,000 | 10,000 | 999.9 | 100.0000% | 100% |
| 10,000/s | 100,000 | 99,986 | 99,986 | 9,998.6 | 99.9860% | 100% |
| 50,000/s | 500,000 | 499,907 | 499,907 | 49,991.3 | 99.9814% | 100% |
| 100,000/s | 1,000,000 | 991,767 | 991,767 | 99,177.0 | 99.1767% | 100% |

These are local loopback results, not a guarantee for remote networks or different machines. The matching sender and receiver totals show no application-level delivery loss in these runs. The growing pacing gap at 100k points to the sender as the first optimization target.

## Plan of attack

1. **Ticker pacing:** Replace one ticker event per packet with elapsed-time pacing and limited catch-up batches. This should recover sends missed during short scheduling delays.
2. **Reporting isolation:** Keep console formatting and output off the packet-sending path. This prevents reporting work from delaying sends at higher rates.
3. **Stream readiness:** Start scheduled timing after streams finish initialization. This gives each stream its full configured duration.
4. **Receiver headroom:** Increase socket buffering or investigate batched reads only when received packets fall below sent packets. Current runs do not show receiver loss.
5. **Benchmark automation:** Run each target multiple times and report median, minimum, and maximum results. Machine-readable output can later feed Markdown reports and the TUI.

## Optimization history

No performance optimizations have been measured yet. Each completed change will record the issue, change, before and after results, and any tradeoffs here.

## Reproducing a run

Start the listener:

```bash
go run ./cmd/trafficcontrol listen --protocol udp --address 127.0.0.1:5000
```

Run one benchmark scenario in another terminal:

```bash
go run ./cmd/trafficcontrol run examples/scenarios/benchmark/udp/100k.yaml
```

After the sender stops, stop the listener with `Ctrl-C` and compare the two summary lines.
