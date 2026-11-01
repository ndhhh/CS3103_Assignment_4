# AI and coding-assistant usage — team review required

**Every team member must disclose their actual tools and check these examples before submission.**

## Tools and purposes

- OpenAI Codex assisted Ong Cheng Leong with repository inspection, Ubuntu test commands, slow-client result analysis, and drafting the submission documentation. The team must add any other AI or coding assistants used by any member, including what each was used for.

## A useful suggestion and verification

The assistant suggested separating normal and deliberately slow clients when interpreting a low final-completeness result. Ong Cheng Leong computed final-caption coverage by role from the supplied CSV output. In the 30%-slow experiment, both roles had identical coverage within each run. This prompted a controlled follow-up using a fixed 76-segment source and a longer 100-second client window. Three independent runs with lag probes enabled then reached 100% final completeness, with zero connection-error events.

## A suggestion that required correction

The assistant initially treated an apparent completeness improvement in a 60-second A/B run as evidence that pacing caused the improvement. That conclusion was premature: the client observation window could end before all generated captions had been delivered on the VM, and normal clients lost finals at the same rate as slow clients in the 30%-slow scenario. The team should not cite that comparison as causal evidence. The corrected experiment kept the source count fixed at 76, extended the client window to 100 seconds, repeated both configurations three times, and compared freshness, latency, completeness, and connection errors. The corrected method and results are recorded in `docs/slow_client_evaluation.md`.

## Responsibility

The team is responsible for every submitted line of code, experimental result, and claim. Every member should be able to explain the design and relevant implementation during the individual viva.
