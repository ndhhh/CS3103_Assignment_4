# LiveRecall

LiveRecall distributes a revision-based caption stream from the supplied deterministic source to browser clients over WebSockets. The browser replaces provisional revisions in place, keeps recent finalized captions locally for recall, reconnects after a connection loss, and recovers missed finals.

## Team

Ng De Heng, Foo Juinn Kai, Jay Kim, and Ong Cheng Leong. Responsibilities and a proposed contribution split are recorded in `CONTRIBUTIONS.md` for team review before submission.

## Ubuntu setup

Run these commands from the repository root on Ubuntu. The network impairment experiments require Linux `tc/netem`; run the server and simulated clients in the same Ubuntu VM.

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv venv
venv/bin/python -m pip install -r requirements.txt
```

The mandatory server path uses only the dependency listed in `requirements.txt`. No ASR, cloud service, or external database is required.

## Start the application

Use three terminals in the repository root:

```bash
# Terminal 1: required server entry point
venv/bin/python solution_server.py --port 8765
```

```bash
# Terminal 2: serve the browser files
venv/bin/python -m http.server 8000 --directory client
```

Open `http://127.0.0.1:8000` in a browser **inside the same Ubuntu VM**. The browser connects to `ws://127.0.0.1:8765`, so opening the page on another machine requires changing that address in `client/app.js`.

```bash
# Terminal 3: deterministic caption source
venv/bin/python caption_source.py --duration 120
```

For a longer visual demonstration, use `--duration 600`. Add `--revision-delay 1.0` if the provisional-to-final replacement needs to be easier to see on video. Automated experiments use the source's default timing.

In the browser, the newest caption appears first. A provisional caption updates in the same row until its final revision arrives. The 15/30/60-second buttons filter the locally held finalized captions; 30 seconds is selected initially. Under **Developer options**, **Simulate disconnect** closes one connection so automatic reconnect and incremental recovery can be observed. Open a new browser tab later in the session to demonstrate late join. **Reset client** clears local state and requests recent server history after reconnecting.

### Show delayed localhost networking in the video

With the server, browser page, and caption source already running **inside the Ubuntu VM**, open a fourth terminal. Keep the source output and browser visible side by side. Apply 400 ms delay with 50 ms jitter to loopback:

```bash
sudo tc qdisc replace dev lo root netem delay 400ms 50ms
tc qdisc show dev lo
```

The source continues producing revisions and the browser should continue showing captions, but they may appear later. Watch a row change from **Updating live** to **Final caption**. The page's connection indicator should normally remain **connected**. This delays packets on `lo` for both the source-to-server and server-to-browser paths; it demonstrates network impairment, not a client that deliberately sleeps between reads. The browser has no slow-mode button.

After roughly 20–30 seconds, remove the rule and verify it is gone:

```bash
sudo tc qdisc del dev lo root
tc qdisc show dev lo
```

New captions should then arrive with less visible delay. If you leave the demo early, still run the delete command. Do not leave `tc/netem` enabled while doing unrelated tests or browsing through localhost.

## Checks and public experiments

Run the quick compatibility check and deterministic correctness checks:

```bash
venv/bin/python submission_check.py
bash tests/run_checks.sh
```

The public experiments below use the supplied measurement logic and restart the server for each of three independent runs. CSV and JSON summaries are written under `results/`.

```bash
# T1: 20 clients, basic correctness
venv/bin/python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem results/t1

# T2: 100 clients, normal network
venv/bin/python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem results/t2

# T3: 100 clients, 10% slow readers
venv/bin/python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem results/t3

# T4: 100 clients, 20% temporary disconnection
venv/bin/python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem results/t4

# Summarize the saved T1-T4 runs
venv/bin/python experiments/compare_runs.py results --only t1,t2,t3,t4
```

For T5, run the loopback impairment suite as a normal user on Ubuntu. It asks for `sudo` to apply `tc` profiles, runs three repetitions per profile, and resets `lo` when it exits. Other loopback experiments should not run at the same time.

```bash
bash experiments/run_impairment_tests.sh
```

The controlled slow-reader before/after comparison and its interpretation are in [docs/slow_client_evaluation.md](docs/slow_client_evaluation.md). The 60-second stress sweep can end before a slow VM's source and clients have settled; that study fixes the source at 76 segments and gives clients 100 seconds.

## Design summary

- `solution_server.py` accepts `SOURCE_HELLO` and `CLIENT_HELLO`, validates revisions, and prevents an older revision from replacing a newer or final state.
- Finalized captions are retained in a bounded history for up to 60 seconds of recovery. A reconnecting or late client receives a `HISTORY` response containing recent finals, then resumes live updates.
- Each client has a separate bounded outbox. Obsolete provisional revisions can be replaced while waiting; final captions are preserved. Slow-reader detection uses application-level ping/pong feedback, and network lag uses TCP feedback on Linux. These controls keep a slow client from blocking normal clients.
- `client/app.js` stores the most recent caption state by segment, renders revisions in place, maintains local recall, and reconnects automatically.

## Submission check

After adding `README.md`, `AI_USAGE.md`, `CONTRIBUTIONS.md`, and `report.pdf`, run:

```bash
venv/bin/python submission_check.py --final
```

This verifies required paths and the server's startup/WebSocket handshake. It does not replace the T1-T5 experiments, browser demo, or report review.
