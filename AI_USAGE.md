# AI and coding-assistant usage

## Tools and purposes

- OpenAI Codex assisted Ong Cheng Leong with repository inspection, Ubuntu test commands, slow-client result analysis, and drafting the submission documentation. 

- ChatGPT Luna-6 assisted Ng De Heng with code quality and application logic checks during the set up of client and server states, as well as caption history handling on new client connections and reconnections. 

- Claude Code (Anthropic) assisted Jay Kim with implementing and refactoring the server's delivery path (per-client outboxes and the two control loops), writing the experiment runners and correctness checks in `experiments/` and `tests/`, running and summarizing the public, impairment and robustness experiments, and Linux TCP details (TCP_INFO, TCP_CORK). Each change was adopted only if it beat the previous version on the same tests.

## A useful suggestion and verification

The assistant suggested separating normal and deliberately slow clients when interpreting a low final-completeness result. Ong Cheng Leong computed final-caption coverage by role from the supplied CSV output. In the 30%-slow experiment, both roles had identical coverage within each run. This prompted a controlled follow-up using a fixed 76-segment source and a longer 100-second client window. Three independent runs with lag probes enabled then reached 100% final completeness, with zero connection-error events.

For the server, Claude Code suggested detecting slow readers with WebSocket ping/pong, counting a missing pong as a stall only once TCP had acknowledged the ping. Jay Kim verified the premise first: a slow reader's advertised TCP window stayed at 65,536 B with about 70 unread messages, because the websockets library reads ahead 16 messages and the kernel buffers the rest, so TCP alone cannot reveal a slow reader. With this loop, T3 slow-reader P50 is 181 ms (14.7 s for the queue-only server) and completeness 100% (95.13%).

## A suggestion that required correction

The assistant initially treated an apparent completeness improvement in a 60-second A/B run as evidence that pacing caused the improvement. That conclusion was premature: the client observation window could end before all generated captions had been delivered on the VM, and normal clients lost finals at the same rate as slow clients in the 30%-slow scenario. The team should not cite that comparison as causal evidence. The corrected experiment kept the source count fixed at 76, extended the client window to 100 seconds, repeated both configurations three times, and compared freshness, latency, completeness, and connection errors. The corrected method and results are recorded in `docs/slow_client_evaluation.md`.

Claude Code also claimed that network impairments could not fool the ping/pong loop, since a ping's ACK travels with its pong. On a congested 2 Mbit/s link, pongs were dropped while later ACKs still arrived, so fast clients were paced by mistake (518 pacing decisions across the network runs). Its proposed fix made things worse: 909 fast clients paced at 500 clients, fresh finals 0%, worst-run completeness 94.3% at 700 clients. Jay Kim reverted it, and the limitation is documented in the report.

## Responsibility

The team is responsible for every submitted line of code, experimental result, and claim. Every member should be able to explain the design and relevant implementation during the individual viva.
