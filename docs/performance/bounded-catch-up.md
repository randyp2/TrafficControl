# Bounded catch-up pacing

## What I learned

I started with a simple assumption: one ticker event should send one packet. Testing at higher rates showed why that breaks down.

In the Go version used by this project, a ticker channel is unbuffered. More importantly, the [`time.Ticker`](https://pkg.go.dev/time#NewTicker) contract allows it to adjust or drop ticks when the receiver is slow. If `Sender.Send` or the Go scheduler delays the stream loop, the ticker does not queue every missed send opportunity for later.

Those are dropped ticker events, not network packets. The old pacer never attempted the missing sends, so they did not reach the point where the network could drop them.

The main delay is around the synchronous `Sender.Send` call. That call hands the payload to the operating system, so part of its latency lives in the OS and network stack. I cannot remove that work from TrafficControl. I needed the pacer to behave correctly when a send takes longer than expected.

## Options I considered

### 1. Keep every missed send in a backlog

I could track the full deficit and send all of it later. In theory, this would eventually reach the requested packet count if the sender catches up.

The problem is the output shape. A delay could create a sudden burst of hundreds or thousands of packets, which is not smooth traffic. If the system stays slower than the target rate, the backlog could also grow forever.

### 2. Use bounded catch-up

I removed the assumption that each timer event represents exactly one packet. The timer now acts only as a wake-up signal. When it fires, the pacer compares the current time with the next packet deadline and calculates how many packets are due.

If the sender is behind, it sends a catch-up batch. `maxCatchUpWindow` limits that batch to the amount of traffic that should fit within 5 ms. This limits the worst burst size. It does not remove bursts completely.

If the sender is 1,000 packets behind but the 5 ms window allows only 750, the pacer sends 750 and discards the older debt. This keeps the stream responsive without creating an unbounded backlog.

This reminds me of a token bucket, which is commonly used by rate limiters. Time creates send credit, a capacity limits how much credit can accumulate, and sends consume the credit. The implementation here is not a full token bucket because it tracks packet deadlines and bounded debt, but the tradeoff is similar.

## Example timeline

Initial configuration:

- Target rate: 1,000 packets per second
- Packet interval: 1 ms
- Maximum catch-up window: 5 ms
- Maximum catch-up batch: 5 packets

| Time | What happens | Packets sent | Next deadline |
| ---: | --- | ---: | ---: |
| 0 ms | Start the pacer | 0 | 1 ms |
| 1 ms | One packet is due, so send 1 | 1 | 2 ms |
| 2 ms | One packet is due, so send 1 | 2 | 3 ms |
| 5 ms | The loop was delayed. Deadlines at 3, 4, and 5 ms are due, so send 3 | 5 | 6 ms |
| 15 ms | Ten packets are due, but the batch limit is 5. Send 5 and discard the older debt | 10 | 16 ms |

The old design would receive only one usable ticker event after the delay and send one packet. The new design calculates the deficit from elapsed time, catches up within a safe bound, and continues from a current deadline.

## Before and after

These results use UDP over local loopback, a 9-byte payload, 10-second runs, and five runs per target. Each value below is the median unless a range is shown.

| Target | Before sent/s | After sent/s | Before target ratio | After target ratio | Before pacing error | After pacing error | Delivery |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000/s | 99,296.0 | 99,999.9 | 99.2954% | 99.9994% | 0.7046% | 0.0006% | 100% |
| 150,000/s | 140,668.7 | 150,014.9 | 93.7791% | 100.0092% | 6.2209% | 0.0092% | 100% |

The bounded pacer fixed the large undershoot at both rates. At 150k, the sender moved from about 9,331 missing sends per second to about 15 extra sends per second.

The result has a CPU tradeoff. Sender efficiency changed from 108,165 to 96,618 packets per CPU-second at 100k, about a 10.7% decrease. At 150k it changed from 126,556 to 125,115 packets per CPU-second, about a 1.1% decrease. The pacer does more work to preserve timing accuracy, so this is worth tracking as I continue optimizing.

## Why 150k currently overshoots

The current interval calculation uses integer nanoseconds:

```text
1,000,000,000 ns / 150,000 packets = 6,666.666... ns per packet
```

`time.Duration` truncates that result to 6,666 ns. That interval represents:

```text
1,000,000,000 ns / 6,666 ns = 150,015.0015 packets per second
```

The measured median of 150,014.9 packets per second closely matches that effective rate. At 100k, the interval is exactly 10,000 ns, so this truncation does not create the same consistent overshoot.

The next pacing improvement is to preserve the fractional remainder instead of representing the whole rate as one truncated packet interval.
