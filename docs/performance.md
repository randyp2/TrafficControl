# Performance

This document records repeatable baselines and the impact of future performance changes.

## Method

- Environment: Apple M4, 10 logical CPUs, 16 GiB memory
- Software: macOS 26.6.2 arm64, Go 1.27.1
- Protocol: UDP over `127.0.0.1`
- Payload: 9 bytes (`benchmark`)
- Duration: 10 seconds
- Rate sweep: one run per target
- Boundary tests: five runs at 100k and 150k
- Listener drain time: 250 milliseconds after the sender stopped

## Metrics

- **Target:** The configured packets-per-second rate.
- **Expected:** The target rate multiplied by the configured duration.
- **Sent:** Packets successfully handed to the operating system by the sender.
- **Received:** Datagrams read by the TrafficControl listener.
- **Average sent/s:** Packets sent divided by the sender's actual runtime.
- **Target ratio:** `sent ÷ expected × 100`. A value below 100% is an undershoot and a value above 100% is an overshoot.
- **Pacing error:** `abs(sent - expected) ÷ expected × 100`. Lower is better. This measures how closely the generator followed the requested rate, not network delivery.
- **Target-ratio range:** The lowest and highest target ratio across repeated runs. A narrow range indicates consistent behavior.
- **Delivery:** `received ÷ sent × 100`. This measures how many reported sends reached the listener during the run.
- **CPU efficiency:** Packets processed per user-plus-system CPU second. Higher values mean less CPU work per packet.
- **Peak memory:** The maximum resident memory reported by `/usr/bin/time -l`, shown as the median across repeated runs.

For example, the median 150k boundary run sent 1,500,138 of 1,500,000 expected packets. That is a 100.0092% target ratio, a 0.0092% pacing error, and 100% observed delivery.

## Initial rate sweep

| Target | Expected | Sent | Received | Average sent/s | Target ratio | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000/s | 10,000 | 10,000 | 10,000 | 999.9 | 100.0000% | 100% |
| 10,000/s | 100,000 | 99,986 | 99,986 | 9,998.6 | 99.9860% | 100% |
| 50,000/s | 500,000 | 499,907 | 499,907 | 49,991.3 | 99.9814% | 100% |
| 100,000/s | 1,000,000 | 991,767 | 991,767 | 99,177.0 | 99.1767% | 100% |

These are local loopback results, not a guarantee for remote networks or different machines. The matching sender and receiver totals show no application-level delivery loss in these runs. The growing pacing gap at 100k points to the sender as the first optimization target.

## Current boundary results

A run passes when pacing error is at most 1% and delivery is at least 99.99%.

| Target | Runs | Median sent/s | Target ratio | Pacing error | Target-ratio range | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 5 | 99,999.9 | 99.9994% | 0.0006% | 99.9963% to 100.0007% | 100% |
| 150,000/s | 5 | 150,014.9 | 100.0092% | 0.0092% | 100.0085% to 100.0190% | 100% |

Both tested rates now pass. The upper sustainable-rate boundary has not been measured yet. Matching sender and receiver totals continue to show no observed delivery loss on local loopback.

## Resource use at the boundary

| Target | Sender packets/CPU-s | Receiver packets/CPU-s | Sender peak memory | Receiver peak memory |
| ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 96,618 | 246,296 | 5.92 MiB | 10.09 MiB |
| 150,000/s | 125,115 | 325,441 | 6.11 MiB | 10.06 MiB |

Values are medians from five runs. Resource usage was collected separately for the sender and listener with `/usr/bin/time -l`.

## Plan of attack

1. **Rate precision:** Remove integer interval truncation so rates such as 150k do not consistently overshoot. The current 6,666 ns interval represents about 150,015 packets per second instead of exactly 150,000.
2. **Reporting isolation:** Keep console formatting and output off the packet-sending path. This prevents reporting work from delaying sends at higher rates.
3. **Stream readiness:** Start scheduled timing after streams finish initialization. This gives each stream its full configured duration.
4. **Receiver headroom:** Increase socket buffering or investigate batched reads only when received packets fall below sent packets. Current runs do not show receiver loss.
5. **Benchmark automation:** Run each target multiple times and report median, minimum, and maximum results. Machine-readable output can later feed Markdown reports and the TUI.

## Optimization history

- [Bounded catch-up pacing](performance/bounded-catch-up.md): replaced one-tick-per-packet pacing with deadline-based batches. Median pacing error fell from 0.7046% to 0.0006% at 100k and from 6.2209% to 0.0092% at 150k.

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
