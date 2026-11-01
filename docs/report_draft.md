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

Run the supplied harness on Ubuntu with server and load clients in the same VM. Use three independent runs per scenario and report medians. Caption Age is client receive time minus source timestamp; report P50/P95/P99, fresh caption and fresh final ratios at the public 500 ms threshold, final completeness, and normal update coverage. T4 also needs recovery timing and duplicates/stale counts. T5 uses `tc/netem` on `lo` and must be reset after each experiment.

**T1–T5 are medians from three-run Ubuntu summaries. T5's main row is the combined delay-and-loss profile; all three T5 profiles are shown below. Do not infer values from the old committed `demo_results.json`.**

| Test | Clients / condition | P50 ms | P95 ms | P99 ms | Fresh % | Fresh finals % | Final completeness % | Coverage % | Additional observation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| T1 | 20, normal | 11 | 30 | 59 | 100.0 | 100.0 | 100.00 | 100.00 | Minimum per run 95.45%; 0 errors/duplicates/stale |
| T2 | 100, normal | 20 | 41 | 52 | 100.0 | 100.0 | 100.00 | 100.00 | Minimum per run 97.96%; 0 errors/duplicates/stale |
| T3 | 100, 10% slow at 500 ms/read | 21 | 50 | 3569 | 98.4 | 96.8 | 100.00 | 94.52 | Minimum 100%; slow-client P50 245 ms; 0 errors/duplicates/stale |
| T4 | 100, 20% disconnect/reconnect | 19 | 38 | 45 | 100.0 | 98.4 | 100.00 | 98.25 | Recovery P95 15.1 ms; minimum 97.37%; 0 errors/duplicates/stale |
| T5 | 100, 120 ± 30 ms delay + 3% IP loss | 293 | 818 | 1247 | 83.6 | 84.6 | 97.94 | 96.22 | Minimum per run 97.82%; 0 errors/duplicates/stale |

T5 ran on Linux loopback with `tc/netem`. Each profile had 100 clients, a 40-second window, and three completed runs. The first T5 folder also contains an incomplete attempt at the combined profile; the completed three-run retry in the second folder is used here.

| T5 profile | P50 ms | P95 ms | P99 ms | Fresh % | Fresh finals % | Final completeness % | Coverage % | Minimum completeness % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 ± 20 ms delay | 222 | 273 | 296 | 100.0 | 100.0 | 100.00 | 100.00 | 100.00 |
| 120 ± 30 ms delay + 3% loss | 293 | 818 | 1247 | 83.6 | 84.6 | 97.94 | 96.22 | 97.82 |
| 3% loss | 26 | 64 | 274 | 99.8 | 85.7 | 85.71 | 86.33 | 83.67 |

No T5 run reported connection errors, duplicate captions, or stale captions. TCP retransmission preserves reliable, ordered WebSocket transport, but it can delay messages and create head-of-line stalls. The combined profile's P95 exceeded the 500 ms freshness threshold and its median final completeness fell below 99%. The loss-only profile's P95 stayed at 64 ms while final completeness fell to 85.71%; a low latency among observed messages therefore did not imply that all expected finals arrived before the 40-second cutoff. We cannot distinguish source timing, retry delay, and server behavior from these aggregate summaries alone; inspect per-run logs and timestamps before assigning a cause.

If the source takes longer than the client window on a constrained VM, report this timing limitation rather than treating unreceived future finals as a server failure. Keep the required public test result as measured, then use a fixed segment count and a longer client window for a controlled follow-up.

T1–T4 all had 100% median final completeness and zero connection-error, duplicate, or stale events in the latest three-run summaries. Individual-run completeness dipped below 99% in T1, T2, and T4, despite those medians. This variability should be disclosed; the cause cannot be assigned to the server without inspecting source timing and per-run logs. T3's P99 reached 3.57 s because the slow-reader population contributes to the tail, while its median P95 remained 50 ms.

## 5. Before/after slow-client comparison

Ong Cheng Leong ran a controlled A/B experiment on Ubuntu: 100 clients, 30% slow readers, 500 ms pause after each message, 76 source segments, 100-second client window, three independent runs per configuration. Lag probes were enabled in the final design and disabled with `LIVERECALL_BACKPRESSURE=0` in the comparison. The fixed source count and longer window followed an initial 60-second sweep in which normal and slow clients had similar incomplete final counts, indicating that the short window distorted completeness.

| Lag probes | Median overall P95 | Median slow-client P95 | Fresh final ratio | Final completeness by run | Connection-error events by run |
|---|---:|---:|---:|---|---|
| Enabled | 1,908 ms | 8,321 ms | 88.95% | 100%, 100%, 100% | 0, 0, 0 |
| Disabled | 35,563 ms | 42,356 ms | 70.39% | 92.5%, 100%, 100% | 7, 30, 30 |

Lag probes lowered median slow-client P95 by about 34 seconds and improved the fresh final ratio by 18.56 percentage points. Normal-client P95 stayed below 30 ms in both configurations. Normal update coverage was 83.68% with probes and 92.73% without; this is consistent with coalescing obsolete provisional revisions during overload. Neither configuration produced duplicate or stale captions in these runs. The comparison is one controlled workload, not a prediction of hidden-test performance.

## 6. Failures, limitations, and future work

The initial 60-second slow-reader sweep produced low apparent completeness, but a per-role breakdown showed that normal and slow clients lost finals at nearly the same rate within each run. Extending the client window while fixing the source at 76 segments restored 100% completeness in all enabled runs. This shows why a load generator's production time and the client's observation window must be checked before attributing missing finals to delivery policy.

Limitations include one Ubuntu VM for the controlled A/B result, only three runs per condition, a deterministic source rather than real speech, and T5 loss-profile completeness below 99%. The T5 summary lacks the event timing needed to attribute the deficit to source throughput, retransmission, server delivery, or the 40-second observation window. Future work could adapt the pace estimate across different client libraries and network paths, inspect per-run loss events, and evaluate a real lecture ASR input without changing the required WebSocket protocol.

## References / appendix (outside the 4–5 page main text)

- CS3103 Assignment 4 LiveRecall brief, updated 17 October 2026.
- Supplied repository harness: `run_demo.py`, `analyse_results.py`, `network_profiles/`, and `experiments/`.
- Reproduction commands and additional A/B data: `docs/slow_client_evaluation.md`.
