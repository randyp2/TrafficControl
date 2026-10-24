# Performance

This document records repeatable baselines and the impact of future performance changes.

## Method

- Environment: Apple M4, 10 logical CPUs, 16 GiB memory
- Software: macOS 26.6.2 arm64, Go 1.27.1
- Protocols: UDP datagrams and TCP byte streams over `127.0.0.1`
- Payload: 9 bytes (`benchmark`)
- Duration: 10 seconds
- Current comparison: five runs per protocol at 100k, 150k, and 200k, alternating UDP and TCP
- TCP sanity sweep: one run each at 1k, 10k, and 50k
- One stream per run; TCP uses one connection
- A single compiled binary, normal reporting enabled, output captured without colors or verbose diagnostics
- Each listener announces readiness before the sender starts; loss and latency are disabled
- Listener drain time: 250 milliseconds after the sender stopped

## Metrics

- **Target:** Generated payloads per second. Each payload becomes one UDP datagram or one TCP write; TCP writes are not TCP segments.
- **Expected:** The target rate multiplied by the configured duration.
- **Generated:** Payload opportunities produced by the pacer, before simulated loss or latency.
- **Sent:** Complete payloads successfully handed to the operating system by the sender.
- **Received:** UDP datagrams or TCP bytes read by the listener. TCP read calls cannot count payloads.
- **Average sent/s:** Successful payload sends divided by the sender's actual runtime, rounded to one decimal in the console summary.
- **Target ratio:** `generated ÷ expected × 100`. A value below 100% is an undershoot and a value above 100% is an overshoot.
- **Pacing error:** `abs(generated - expected) ÷ expected × 100`. This compares total generation with the configured ten-second budget; it includes startup and stop-boundary effects. It does not measure per-packet timing accuracy. Historical zero-loss UDP results used sent counts.
- **Target-ratio range:** The lowest and highest target ratio across repeated runs. A narrow range indicates consistent behavior.
- **Delivery:** UDP uses `received datagrams ÷ sent datagrams × 100`; TCP uses `received bytes ÷ sent bytes × 100`. Matching TCP totals show observed byte delivery, not a packet-loss or retransmission measurement.
- **CPU efficiency:** Successful payload sends per sender user-plus-system CPU second. Higher values mean less CPU work per send.
- **Peak memory:** Maximum resident memory for each sender/listener process. Current runs use OS child-process resource usage; older measurements used `/usr/bin/time -l`.

For example, sending 1,499,990 of 1,500,000 expected payloads with no simulated loss gives a 99.9993% target ratio and 0.0007% pacing error. Delivery is checked separately at the listener.

## Current UDP and TCP results (2026-10-24)

All 33 runs met the existing acceptance thresholds: at most 1% pacing error and at least 99.99% observed delivery. UDP datagram counts matched every reported send, and TCP byte counts matched every reported sent byte. Every TCP run ended with one accepted connection, zero active connections, and zero read errors.

| Protocol | Target payloads/s | Runs | Median sent/s | Median pacing error | Pacing-error range | Delivery |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| TCP | 1,000 | 1 | 999.5 | 0.05000% | single run | 100% |
| TCP | 10,000 | 1 | 10,000.0 | 0.00300% | single run | 100% |
| TCP | 50,000 | 1 | 49,999.9 | 0.00280% | single run | 100% |
| UDP | 100,000 | 5 | 99,999.8 | 0.00170% | 0.00080% to 0.00890% | 100% |
| TCP | 100,000 | 5 | 99,999.8 | 0.00420% | 0.00300% to 0.00650% | 100% |
| UDP | 150,000 | 5 | 149,999.8 | 0.00167% | 0.00160% to 0.00207% | 100% |
| TCP | 150,000 | 5 | 149,999.8 | 0.00273% | 0.00267% to 0.01580% | 100% |
| UDP | 200,000 | 5 | 199,999.3 | 0.00185% | 0.00150% to 0.00520% | 100% |
| TCP | 200,000 | 5 | 199,999.7 | 0.00355% | 0.00295% to 0.03390% | 100% |

The lower TCP targets are single sanity runs; the 100k, 150k, and 200k rows are five-run medians. The upper capacity limit was not reached. TCP's worst repeated run had 0.03390% count error at 200k, so the median alone would hide some variation.

These count errors are slightly higher than the older UDP figures below, while measured send rates remain close to target. The scenario clock starts before stream initialization, and immediate cancellation can interrupt the last send. No timing traces were collected, so the differences cannot be attributed to one bottleneck. At shutdown, generated payloads can exceed successful sends even with no simulated drops; delayed or interrupted payloads are not intentional loss.

### Current resource use

| Protocol | Target payloads/s | Sender sends/CPU-s | Sender peak memory | Receiver peak memory |
| --- | ---: | ---: | ---: | ---: |
| UDP | 100,000 | 95,480 | 6.19 MiB | 9.81 MiB |
| TCP | 100,000 | 96,890 | 6.25 MiB | 5.86 MiB |
| UDP | 150,000 | 126,518 | 6.13 MiB | 10.28 MiB |
| TCP | 150,000 | 125,222 | 6.25 MiB | 5.80 MiB |
| UDP | 200,000 | 147,343 | 6.22 MiB | 11.02 MiB |
| TCP | 200,000 | 149,987 | 6.19 MiB | 5.70 MiB |

Resource values are medians from five runs. CPU time is user plus system time; memory is each process's maximum resident size. Raw counts, rates, CPU time, and memory for every run are in [tcp-udp-baseline.csv](performance/tcp-udp-baseline.csv).

## Historical UDP rate sweep

| Target | Expected | Sent | Received | Average sent/s | Target ratio | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000/s | 10,000 | 10,000 | 10,000 | 999.9 | 100.0000% | 100% |
| 10,000/s | 100,000 | 99,986 | 99,986 | 9,998.6 | 99.9860% | 100% |
| 50,000/s | 500,000 | 499,907 | 499,907 | 49,991.3 | 99.9814% | 100% |
| 100,000/s | 1,000,000 | 991,767 | 991,767 | 99,177.0 | 99.1767% | 100% |

These measurements predate bounded catch-up pacing. The growing pacing gap at 100k identified the sender as the first optimization target.

## Historical UDP boundary results

A run passes when pacing error is at most 1% and delivery is at least 99.99%.

| Target | Runs | Median sent/s | Target ratio | Pacing error | Target-ratio range | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 5 | 99,999.9 | 99.9994% | 0.0006% | 99.9963% to 100.0007% | 100% |
| 150,000/s | 5 | 149,999.8 | 99.9993% | 0.0007% | 99.9880% to 100.0002% | 100% |
| 200,000/s | 5 | 199,999.9 | 99.9993% | 0.0007% | 99.9991% to 99.9997% | 100% |

These are the five-run results after bounded catch-up and fractional deadline precision, before TCP integration. They remain here for comparison with the current measurements.

## Historical UDP resource use

| Target | Sender packets/CPU-s | Receiver packets/CPU-s | Sender peak memory | Receiver peak memory |
| ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 96,618 | 246,296 | 5.92 MiB | 10.09 MiB |
| 150,000/s | 126,141 | 325,441 | 6.03 MiB | 10.06 MiB |

Values are medians from the earlier five-run UDP baseline. Resource usage was collected separately for the sender and listener with `/usr/bin/time -l`.

## Plan of attack

1. **Stream readiness:** Start scheduled timing after streams finish initialization. TCP setup currently consumes some of the scenario's configured time.
2. **Representative workloads:** Test larger payloads, multiple streams, and remote hosts before treating small loopback payloads as a general capacity limit.
3. **Reporting isolation:** Move console formatting off the sending path if profiling shows it delays traffic. Keep the existing simple reporter until there is evidence.
4. **Higher-rate boundary:** Continue above 200k only when more headroom is needed; the current sweep does not establish the upper limit.
5. **Receiver headroom:** Investigate socket buffering or batched reads when receiver totals fall below reported sends.

## Optimization history

- [Bounded catch-up pacing](performance/bounded-catch-up.md): replaced one-tick-per-packet pacing with deadline-based batches. Median pacing error fell from 0.7046% to 0.0006% at 100k and from 6.2209% to 0.0007% at 150k.
- [Fractional deadline precision](performance/bounded-catch-up.md#what-i-learned-about-nanosecond-precision): carried truncated nanoseconds into future deadlines, removing the repeatable 150k overshoot.
- **TCP lifecycle:** Added cancelable connection setup, bounded writes, and scheduled stops that interrupt blocked writes. The receiver shares UDP's reporting loop and counts bytes across concurrent connections.

## Reproducing a run

Start the listener:

```bash
go build -o /private/tmp/trafficcontrol ./cmd/trafficcontrol
/private/tmp/trafficcontrol listen --protocol udp --address 127.0.0.1:5000
```

Run one benchmark scenario in another terminal:

```bash
/private/tmp/trafficcontrol run examples/scenarios/benchmark/udp/100k.yaml
```

For TCP, use `listen --protocol tcp --address 127.0.0.1:6000` and `examples/scenarios/benchmark/tcp/100k.yaml`. TCP scenarios are available at 1k, 10k, 50k, 100k, 150k, and 200k.

After the sender stops, allow 250ms for the listener to drain, then stop it with Ctrl-C. Compare UDP datagram totals or TCP byte totals. Run each target five times and report medians and ranges; avoid building or running other tests during measurement.

These are local loopback measurements with nine-byte payloads, not guarantees for remote networks, larger payloads, or different machines.
