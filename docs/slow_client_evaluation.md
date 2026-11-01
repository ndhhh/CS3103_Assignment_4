# Slow-client evaluation

Tested on an Ubuntu VM with 100 clients, 30% slow readers (500 ms pause after each message), 76 source segments, a 100-second client window, and three independent runs per configuration.

| Configuration | Overall P95 | Slow-client P95 | Fresh finals | Final completeness by run | Connection errors by run |
|---|---:|---:|---:|---|---|
| Lag probes enabled | 1,908 ms | 8,321 ms | 88.95% | 100%, 100%, 100% | 0, 0, 0 |
| Lag probes disabled | 35,563 ms | 42,356 ms | 70.39% | 92.5%, 100%, 100% | 7, 30, 30 |

Normal-client P95 remained below 30 ms in both configurations. Neither configuration recorded duplicate or stale captions. Normal update coverage was 83.68% with probes and 92.73% without probes, consistent with coalescing intermediate revisions for slow readers.

The earlier 60-second runs showed similar final completeness for normal and slow clients within each run. Keeping the source at 76 segments and extending the client window to 100 seconds produced 100% completeness in all three runs with probes enabled. This indicates that the shorter test window distorted the earlier completeness measurement.

Reproduce the enabled run:
`venv/bin/python run_demo.py --clients 100 --duration 100 --source-segments 76 --slow-percent 30 --slow-ms 500 --runs 3 --output-stem results/slow30_100s_76segments`

Reproduce the disabled run by adding `LIVERECALL_BACKPRESSURE=0` before that command and changing the output stem.

These measurements were collected on one VM; latency may vary on other machines.
