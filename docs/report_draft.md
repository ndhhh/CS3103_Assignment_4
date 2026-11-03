# LiveRecall: revision-aware delivery under slow clients and impaired networks

**Working report source.** T1–T5 three-run summaries are now included. Confirm team details and review the final PDF layout before submission. Target length: 4–5 pages excluding references/appendices.

Team: Ng De Heng, Foo Juinn Kai, Jay Kim, Ong Cheng Leong. Confirm spelling and add student IDs if required by the course submission form.

## 1. Architecture and protocol

The deterministic `caption_source.py` connects to `solution_server.py` over WebSocket, sends `SOURCE_HELLO`, and then sends `CAPTION_UPDATE` messages containing a session, segment, revision, source timestamp, `final` flag, and text. The server validates each update and retains only a newer revision for a segment that is not yet final. Each accepted update is offered to independent per-client outboxes. Browser clients connect with `CLIENT_HELLO` and receive recent finalized captions in `HISTORY` before live updates.

The browser stores the latest accepted caption by segment, replaces provisional text in the same row, distinguishes provisional and final state, and retains recent finals locally. The recall window defaults to 30 seconds and can be changed to 15 or 60 seconds without a server request during normal operation.

**Figure to draw for the final PDF:** source → validation/session state → per-client outboxes → WebSocket/TCP → browser state and recall. Label the bounded history and the two delivery feedback loops.

## 2. Revision correctness and bounded recovery

`SessionState.accept` rejects stale or duplicate revisions and never lets an update overwrite a final caption. The server retains finalized captions in a bounded recent history and serves at most 60 seconds of recovery context. A client reports its latest finalized `(last_segment, last_revision)` cursor; the server returns only newer finals in oldest-first order. The browser deduplicates the received history against its local segment state. Initial join and reconnect use the same protocol; a late joiner starts with cursor zero. The server limits per-client unsent captions and closes a hopelessly lagging connection so it can resume from bounded history rather than retaining an unbounded backlog.

The deterministic server checks in `tests/run_checks.sh` passed on Ong Cheng Leong's Ubuntu VM (six of six checks). Record the exact commit and any additional browser checks in the final report.

## 3. Semantic backpressure and overload policy

An ordinary client receives the full revision stream while it can keep up. Under network lag, TCP acknowledgement feedback delays provisional revisions while finals can proceed. For an application that reads slowly despite TCP delivery, ping/pong feedback detects when the WebSocket library has stopped reading. That client is temporarily held, then paced near its observed reading rate. Unsent superseded provisional revisions can be coalesced; finals are retained. When appropriate, several pending finals are bundled into a catch-up `HISTORY` frame. Each client has its own sender task, so a slow reader does not block the source handler or normal clients.

State the trade-off clearly: coalescing lowers normal update coverage for affected slow clients, while protecting useful final state and freshness. The policy must still return to full revision delivery when pressure ends. Bounded queues and recent history prevent unlimited memory growth.

## 4. Public experimental method and results

The supplied harness, unmodified, starts a fresh server for each run. All runs used native Ubuntu 26.04 (Linux 7.0, 16 cores) with server, source and clients on one machine; T5 uses `tc/netem` on `lo`. Three runs per scenario, medians reported. Caption Age is client receive time minus source timestamp; freshness uses the public 500 ms threshold.

| Test | Condition | P50 ms | P95 ms | P99 ms | Fresh % | Fresh finals % | Completeness % | Coverage % |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| T1 | 20 clients | 1.2 | 1.9 | 2.1 | 100.0 | 100.0 | 100.00 | 100.00 |
| T2 | 100 clients | 3.3 | 6.1 | 6.9 | 100.0 | 100.0 | 100.00 | 100.00 |
| T3 | 100, 10% slow (500 ms/read) | 3.1 | 6.1 | 3603 | 98.4 | 97.0 | 100.00 | 94.39 |
| T4 | 100, 20% disconnect for 5 s | 2.9 | 5.1 | 6.2 | 100.0 | 98.4 | 100.00 | 98.25 |

Every run reached 100% completeness, with no duplicate, stale or connection-error events. T3: normal clients 100% fresh; slow readers P50 181 ms, P95 7.5 s. T4: recovery P95 3.4 ms.

| T5 profile (100 clients, 40 s) | P50 ms | P95 ms | P99 ms | Fresh % | Fresh finals % | Completeness % | Coverage % |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 ± 20 ms delay | 202.6 | 249.5 | 266.6 | 100.0 | 100.0 | 100.00 | 100.00 |
| 120 ± 30 ms delay + 3% loss | 252.8 | 690.9 | 959.8 | 91.3 | 87.8 | 100.00 | 97.97 |
| 3% loss | 3.4 | 154.3 | 217.2 | 100.0 | 100.0 | 100.00 | 99.93 |

Delay shifts the whole distribution by about one round trip. Loss only hurts the tail: a lost segment is resent after TCP's 200 ms minimum retransmission timeout, and every caption behind it waits (head-of-line blocking), so P95/P99 sit near that timeout.

## 5. Before/after comparison

Before: the team's earlier queue-based server (PR 8), which reacts only when its own outbox backs up. After: the final design. Same machine and harness, 3 runs each.

| Scenario | Completeness % (before → after) | Key metric (before → after) |
|---|---|---|
| T3, 10% slow readers | 95.13 → 100.00 | slow-reader P50 14.7 s → 181 ms |
| 30% slow readers | 85.39 → 100.00 | slow-reader P50 14.7 s → 204 ms |
| Slow readers, 300 clients on 2 Mbit/s | 95.00 → 100.00 | slow-reader P50 14.6 s → 482 ms |
| 700 clients on 2 Mbit/s | 89.51 → 99.99 | P95 7.0 s → 2.0 s |
| 300 clients on 2 Mbit/s | 100 → 100 | P50 71 → 370 ms (regression, section 6) |

A slow reader's websockets library and kernel buffers absorb thousands of ~42 B messages before the earlier server's outbox backs up, so its slow readers ended about 15 s behind and missed finals. The final design detects the pause at the library through ping/pong and paces the reader. An earlier controlled study of the lag probes on a VM is in `docs/slow_client_evaluation.md`.

## 6. Failures and trade-offs

What did not work (each measured, then dropped):
- **Receive-window signal.** A slow reader's advertised TCP window stayed at 65,536 B with about 70 unread messages (128 KB buffers, ~42 B messages), so TCP cannot reveal a slow reader. Hence the ping/pong loop.
- **TCP_NOTSENT_LOWAT.** Keeping unsent data in the outbox, where it can be replaced, gave nothing on a 2 Mbit/s bottleneck: the backlog was already in flight, not unsent.
- **One ping per 8 messages.** The pause point was known only to within 8 messages, so the server overestimated what was still queued after a stall (12 vs about 8). Readers taking 2 s per message idled about 10 s and missed the last final (99.87%). A ping on every 4th message, with the stall timed from the middle of the uncertainty interval, fixed this (100%; T3 slow-reader P50 222 → 181 ms).
- **Thresholds from TCP's retransmission timeout and the caption stream's rate.** On a congested link a late pong includes network delay, so the reading rate implied by a stall is wrong: 909 fast clients were paced at 500 clients, and worst-run completeness fell to 94.3% at 700 clients. Reverted.

Trade-offs we kept:
- **Slow readers** get fewer provisionals but current text: T3 coverage 94.4% (queue-only server 95.2%), slow-reader P50 181 ms (14.7 s), completeness 100% (95.13%).
- **Under overload**, provisionals wait in the outbox and are replaced while finals go out: coverage 61% vs 99% at 500 clients on 2 Mbit/s, but completeness 99.99% vs 89.5% at 700 clients.
- **Probe precision costs bandwidth.** Each ping adds a pong and its ACK; at 300 clients on 2 Mbit/s P50 is 370 ms vs 71 ms.
- **A slow reader's first ~15–20 s are stale** (8% fresh, then about 100%), because its library buffers 16 messages before any signal reaches the server. Shortening this would mean throttling every new client, which the coverage check penalizes.
- **Lost pongs under heavy overload** still cause false holds (518 fast-client pacing decisions across our network runs).

## 7. Limitations and future work

Results come from one machine with three runs per condition and a deterministic source; the hidden workload may combine conditions we did not test. Both loops read Linux TCP_INFO: on other platforms the network gate is off and a ping still in flight counts as a stall. Future work: a pause signal that survives congestion (for example, reading-progress reports from the client), pinging less often once a client has proven fast, and a real lecture speech recognizer behind the same protocol.

## References / appendix (outside the 4–5 page main text)

- CS3103 Assignment 4 LiveRecall brief, updated 17 October 2026.
- Supplied repository harness: `run_demo.py`, `analyse_results.py`, `network_profiles/`, and `experiments/`.
- Reproduction commands and additional A/B data: `docs/slow_client_evaluation.md`.
