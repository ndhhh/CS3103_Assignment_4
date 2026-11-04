#!/usr/bin/env python3
"""Build a LiveRecall report preview or the validated final report.

The preview visibly marks missing public results. For the final report, supply
docs/report_data.json with T1-T5 objects containing the fields in PUBLIC_KEYS.
Do not copy preview output to report.pdf.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Flowable,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parent.parent
PUBLIC_KEYS = ("condition", "p50", "p95", "p99", "fresh", "fresh_finals", "complete", "coverage")
TESTS = ("T1", "T2", "T3", "T4", "T5")
PENDING = {key: "pending" for key in PUBLIC_KEYS}

INK = colors.black
MARGIN = 23 * mm
TEXT_WIDTH = A4[0] - 2 * MARGIN - 12  # Platypus frame has 6 pt padding on each side.


def register_times(font_dir=None):
    """Embed actual Times New Roman; never substitute a similar typeface."""
    candidates = [
        Path(font_dir) if font_dir else None,
        Path(os.environ["TNR_FONT_DIR"]) if os.environ.get("TNR_FONT_DIR") else None,
        Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts",
        Path("/Library/Fonts"),
        Path("/System/Library/Fonts/Supplemental"),
        Path("/usr/share/fonts/truetype/msttcorefonts"),
        Path.home() / ".local/share/fonts",
    ]
    variants = (
        ("TNR", ("times.ttf", "Times_New_Roman.ttf", "Times New Roman.ttf")),
        ("TNR-Bold", ("timesbd.ttf", "Times_New_Roman_Bold.ttf", "Times New Roman Bold.ttf")),
        ("TNR-Italic", ("timesi.ttf", "Times_New_Roman_Italic.ttf", "Times New Roman Italic.ttf")),
        ("TNR-BoldItalic", ("timesbi.ttf", "Times_New_Roman_Bold_Italic.ttf", "Times New Roman Bold Italic.ttf")),
    )
    for font_name, names in variants:
        font_path = next((folder / name for folder in candidates if folder
                          for name in names if (folder / name).is_file()), None)
        if font_path is None:
            raise SystemExit("Times New Roman fonts are required. Use --font-dir or TNR_FONT_DIR.")
        pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
    pdfmetrics.registerFontFamily(
        "TNR", normal="TNR", bold="TNR-Bold", italic="TNR-Italic", boldItalic="TNR-BoldItalic"
    )


class Architecture(Flowable):
    """Small system flow diagram that stays sharp in the PDF."""

    def __init__(self, width: float):
        super().__init__()
        self.width = width
        self.height = 92

    def draw(self):
        canvas = self.canv
        gap = 20
        box_width = (self.width - 2 * gap) / 3
        boxes = [
            ("Caption source", "deterministic revisions"),
            ("LiveRecall server", "state + bounded outboxes"),
            ("Browser clients", "live + local recall"),
        ]
        canvas.setStrokeColor(INK)
        canvas.setFillColor(INK)
        canvas.setLineWidth(0.6)
        for index, (title, subtitle) in enumerate(boxes):
            x = index * (box_width + gap)
            canvas.rect(x, 18, box_width, 56, fill=0, stroke=1)
            canvas.setFont("TNR-Bold", 10)
            canvas.drawCentredString(x + box_width / 2, 51, title)
            canvas.setFont("TNR", 9)
            canvas.drawCentredString(x + box_width / 2, 35, subtitle)
            if index < 2:
                start = x + box_width + 3
                end = x + box_width + gap - 3
                canvas.line(start, 46, end, 46)
                canvas.line(end, 46, end - 4, 49)
                canvas.line(end, 46, end - 4, 43)


def styles():
    base = getSampleStyleSheet()
    common = dict(fontName="TNR", textColor=INK)
    specs = [
        ("CoverCourse", dict(fontSize=14, leading=19, alignment=TA_CENTER, spaceAfter=10)),
        ("CoverTitle", dict(fontName="TNR-Bold", fontSize=26, leading=31, alignment=TA_CENTER, spaceAfter=16)),
        ("CoverSubtitle", dict(fontSize=15, leading=21, alignment=TA_CENTER)),
        ("CoverAuthors", dict(fontSize=12, leading=21, alignment=TA_CENTER)),
        ("CoverDate", dict(fontSize=12, leading=17, alignment=TA_CENTER)),
        ("Section", dict(fontName="TNR-Bold", fontSize=13, leading=16, spaceBefore=14, spaceAfter=8, keepWithNext=True)),
        ("Subsection", dict(fontName="TNR-Bold", fontSize=11, leading=14, spaceBefore=9, spaceAfter=5, keepWithNext=True)),
        ("Body", dict(fontSize=11, leading=14.3, alignment=TA_JUSTIFY, spaceAfter=7, allowWidows=0, allowOrphans=0)),
        ("BulletBody", dict(fontSize=11, leading=14.3, alignment=TA_JUSTIFY, leftIndent=12, firstLineIndent=-9, spaceAfter=6, allowWidows=0, allowOrphans=0)),
        ("Note", dict(fontSize=9, leading=11.5, alignment=TA_LEFT, spaceAfter=7)),
        ("Caption", dict(fontSize=10, leading=12, alignment=TA_LEFT, spaceBefore=6, spaceAfter=6, keepWithNext=True)),
        ("TableHead", dict(fontName="TNR-Bold", fontSize=8.5, leading=10, alignment=TA_CENTER)),
        ("TableCell", dict(fontSize=9, leading=11, alignment=TA_CENTER)),
        ("TableLeft", dict(fontSize=9, leading=11, alignment=TA_LEFT)),
        ("TableHeadLeft", dict(fontName="TNR-Bold", fontSize=8.5, leading=10, alignment=TA_LEFT)),
    ]
    for name, options in specs:
        base.add(ParagraphStyle(name=name, parent=base["Normal"], **{**common, **options}))
    return base


def para(text: str, style: str = "Body"):
    return Paragraph(text, STYLES[style])


def bullet(text: str):
    return para("•  " + text, "BulletBody")


def table(rows, widths, first_header=True):
    scale = TEXT_WIDTH / sum(widths)
    widths = [width * scale for width in widths]
    prepared = []
    left_columns = {0, 1} if len(rows[0]) == 9 else {0, 3} if len(rows[0]) == 6 else {0}
    for i, row in enumerate(rows):
        prepared.append([
            para(str(value), ("TableHeadLeft" if j in left_columns else "TableHead")
                 if first_header and i == 0 else ("TableLeft" if j in left_columns else "TableCell"))
            for j, value in enumerate(row)
        ])
    obj = Table(prepared, colWidths=widths, hAlign="LEFT", repeatRows=1 if first_header else 0)
    obj.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEABOVE", (0, 0), (-1, 0), 0.8, INK),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, INK),
        ("LINEBELOW", (0, -1), (-1, -1), 0.8, INK),
    ]))
    return obj


def load_public(preview: bool) -> dict:
    path = ROOT / "docs" / "report_data.json"
    if not path.exists():
        if preview:
            return {test: dict(PENDING) for test in TESTS}
        raise SystemExit("Missing docs/report_data.json. Run T1-T5 and fill all report data before building report.pdf.")
    data = json.loads(path.read_text(encoding="utf-8"))
    for test in TESTS:
        if test not in data or any(not str(data[test].get(key, "")).strip() or str(data[test][key]).lower() == "pending" for key in PUBLIC_KEYS):
            if not preview:
                raise SystemExit(f"Incomplete {test} in docs/report_data.json")
            data[test] = {**PENDING, **data.get(test, {})}
    return data


def footer(canvas, doc, preview: bool):
    if doc.page == 1:
        return
    canvas.saveState()
    canvas.setFont("TNR", 10)
    canvas.setFillColor(INK)
    canvas.drawCentredString(A4[0] / 2, 32, str(doc.page - 1))
    if preview:
        canvas.setFont("TNR", 8)
        canvas.drawString(MARGIN, 32, "DRAFT - T1-T5 DATA PENDING")
    canvas.restoreState()


def build(preview: bool, output_path=None):
    public = load_public(preview)
    output = Path(output_path) if output_path else (ROOT / "docs" / "report_preview.pdf" if preview else ROOT / "report.pdf")
    output.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(output), pagesize=A4, rightMargin=MARGIN, leftMargin=MARGIN, topMargin=MARGIN, bottomMargin=MARGIN,
                            title="LiveRecall: revision-aware delivery", author="CS3103 LiveRecall team")
    width = TEXT_WIDTH
    story = [
        Spacer(1, 70),
        para("CS3103", "CoverCourse"),
        para("Assignment 4 Report", "CoverCourse"),
        Spacer(1, 42),
        para("LiveRecall", "CoverTitle"),
        para("Revision-aware live captions with<br/>bounded recovery and per-client delivery", "CoverSubtitle"),
        Spacer(1, 72),
        para("Ng De Heng<br/>Foo Juinn Kai<br/>Jay Kim<br/>Ong Cheng Leong", "CoverAuthors"),
        Spacer(1, 65),
        para("November 2026", "CoverDate"),
        PageBreak(),
    ]

    # Main text starts at page 1 after the unnumbered cover.
    story += [para("1  System architecture", "Section"),
              para("The supplied deterministic source emits provisional revisions and a final caption for each segment. A WebSocket server validates and distributes that stream to many browser clients. The browser replaces revisions in place, retains recent finals, and provides a local 15/30/60-second recall view.")]
    story += [Architecture(width), Spacer(1, 5), para("Figure 1. Required data path. The server keeps bounded session history and separate per-client outboxes.", "Note")]
    story += [para("Protocol", "Subsection"),
              bullet("The source sends <b>SOURCE_HELLO</b>, then <b>CAPTION_UPDATE</b> objects with session, segment, revision, source timestamp, final flag, and text."),
              bullet("A student sends <b>CLIENT_HELLO</b> with the latest finalized segment/revision and a requested history window. The server replies with oldest-first <b>HISTORY</b>, then live updates."),
              bullet("The browser stores one current caption per segment, distinguishes provisional and final text, reconnects automatically, and requests only recent missed finals after a gap."),
              ]

    # Correctness follows the architecture; backpressure starts a new page.
    story += [para("2  Correctness and recovery", "Section"),
              para("The server accepts a revision only when it is newer than the stored revision and the segment has not already become final. This prevents stale updates from overwriting completed text. Final captions enter a bounded recent-history deque. On reconnect, the client sends its latest finalized cursor; the server returns only newer finals within the 60-second recovery limit. The browser deduplicates history against local segment state."),
              para("A late joiner starts with cursor zero and receives recent context before live updates. Snapshotting history and subscribing a client occur without an intervening await, avoiding a gap or duplicate around the handoff. The browser holds at least 60 seconds of recent finalized captions locally for ordinary recall."),
              PageBreak(),
              para("3  Semantic backpressure", "Section"),
              para("Each client has a separate bounded outbox and sender task, so one slow receiver cannot block the source handler or normal readers. Under normal conditions the full revision stream is forwarded. When the network path falls behind, TCP acknowledgement feedback holds provisional revisions while finals proceed. Application-level ping/pong feedback detects a reader that has stopped consuming even when TCP has accepted bytes; that reader is held and then paced near its measured reading rate."),
              bullet("Unsent obsolete provisional revisions can be replaced with the latest state of their segment; finalized captions remain durable."),
              bullet("Several pending finals may be bundled in one catch-up HISTORY frame, reducing the number of reads a slow client must perform."),
              bullet("Per-client pending work is capped; a client that falls too far behind is closed so it can resume through bounded history rather than growing an unlimited queue."),
              para("Trade-off", "Subsection"),
              para("Coalescing lowers provisional update coverage during overload but protects final-caption delivery and freshness. The control loops should return to full-update delivery once lag clears. The browser demo separately verifies revision replacement, local recall, reconnect, and late join."),
              ]

    # Public test methodology and tables.
    story += [para("4  Public experimental method", "Section"),
              para("The supplied harness, unmodified, starts a fresh server for each run and logs caption receipt by simulated clients. All runs used native Ubuntu 26.04 (Linux 7.0, 16 cores) with server, source and clients on one machine; T5 applies Linux tc/netem to loopback. Each scenario has three independent runs and median metrics. Caption Age is client receive time minus source timestamp. Freshness uses the public 500 ms threshold."),
              para("Table 1. Required public tests", "Caption")]
    rows = [["Test", "Condition", "P50", "P95", "P99", "Fresh", "Fresh<br/>finals", "Compl.", "Cover."]]
    for test in TESTS:
        item = public[test]
        rows.append([test, item["condition"], item["p50"], item["p95"], item["p99"], item["fresh"], item["fresh_finals"], item["complete"], item["coverage"]])
    story += [table(rows, [31, 119, 48, 48, 48, 50, 50, 54, 51]), Spacer(1, 9),
              para("Latencies in ms.", "Note"),
              PageBreak(),
              para("Table 2. T5 network profiles", "Caption")]
    profiles = public.get("T5_profiles", {})
    if not profiles and not preview:
        raise SystemExit("Missing T5_profiles in docs/report_data.json")
    profile_rows = [["Profile", "P50", "P95", "P99", "Fresh", "Fresh<br/>finals", "Compl.", "Cover."]]
    for key in ("delay_100ms", "delay_loss", "loss_3pct"):
        item = profiles.get(key, PENDING)
        profile_rows.append([item.get("profile", "pending"), item["p50"], item["p95"], item["p99"],
                             item["fresh"], item["fresh_finals"], item["complete"], item["coverage"]])
    story += [table(profile_rows, [151, 49, 49, 49, 50, 50, 51, 50]), Spacer(1, 8)]

    # Comparison follows network results; failures start the final body page.
    story += [para("5  Before/after comparison", "Section"),
              para("Before: the team's earlier queue-based server (PR 8), which reacts only when its own outbox backs up. After: the final design. Same machine and harness, three runs each."),
              para("Table 3. Before/after comparison", "Caption"),
              table([
                  ["Scenario", "Compl.<br/>before", "Compl.<br/>after", "Metric", "Before", "After"],
                  ["T3, 10% slow readers", "95.13%", "100.00%", "Slow-reader P50", "14.7 s", "181 ms"],
                  ["30% slow readers", "85.39%", "100.00%", "Slow-reader P50", "14.7 s", "204 ms"],
                  ["Slow readers, 300 clients, 2 Mbit/s", "95.00%", "100.00%", "Slow-reader P50", "14.6 s", "482 ms"],
                  ["700 clients, 2 Mbit/s", "89.51%", "99.99%", "P95", "7.0 s", "2.0 s"],
                  ["300 clients, 2 Mbit/s", "100%", "100%", "P50", "71 ms", "370 ms"],
              ], [150, 62, 62, 85, 70, 70]),
              Spacer(1, 9),
              para("A slow reader's websockets library and kernel buffers absorb thousands of ~42-byte messages before the earlier server's outbox backs up, so its slow readers ended about 15 s behind and missed finals. The final design detects the pause at the library through ping/pong and paces the reader. The last row is a regression, explained below."),
              PageBreak(),
              para("6  Failures and trade-offs", "Section"),
              para("What did not work (each measured, then dropped)", "Subsection"),
              bullet("<b>Receive-window signal.</b> A slow reader's advertised TCP window stayed at 65,536 B with about 70 unread messages (128 KB buffers, ~42-byte messages), so TCP cannot reveal a slow reader. Hence the ping/pong loop."),
              bullet("<b>TCP_NOTSENT_LOWAT.</b> Keeping unsent data in the outbox, where it can be replaced, gave nothing on a 2 Mbit/s bottleneck: the backlog was already in flight, not unsent."),
              bullet("<b>One ping per 8 messages.</b> The pause point was known only to within 8 messages, so the server overestimated what was still queued after a stall (12 vs about 8). Readers taking 2 s per message idled about 10 s and missed the last final (99.87%). A ping on every 4th message, with the stall timed from the middle of the uncertainty interval, fixed this (100%; T3 slow-reader P50 from 222 to 181 ms)."),
              bullet("<b>Thresholds from TCP's retransmission timeout and the caption stream's rate.</b> On a congested link a late pong includes network delay, so the reading rate implied by a stall is wrong: 909 fast clients were paced at 500 clients, and worst-run completeness fell to 94.3% at 700 clients. Reverted."),
              para("Trade-offs we kept", "Subsection"),
              bullet("<b>Slow readers</b> get fewer provisionals but current text: T3 coverage 94.4% (queue-only server 95.2%), slow-reader P50 181 ms (14.7 s), completeness 100% (95.13%)."),
              bullet("<b>Under overload</b>, provisionals wait in the outbox and are replaced while finals go out: coverage 61% vs 99% at 500 clients on 2 Mbit/s, but completeness 99.99% vs 89.5% at 700 clients."),
              bullet("<b>Probe precision costs bandwidth.</b> Each ping adds a pong and its ACK; at 300 clients on 2 Mbit/s P50 is 370 ms vs 71 ms."),
              bullet("<b>A slow reader's first 15-20 s are stale</b> (8% fresh, then about 100%), because its library buffers 16 messages before any signal reaches the server. Shortening this would mean throttling every new client, which the coverage check penalizes."),
              bullet("<b>Lost pongs under heavy overload</b> still cause false holds (518 fast-client pacing decisions across our network runs)."),
              para("7  Limitations and future work", "Section"),
              bullet("<b>Experimental scope.</b> Results come from one machine with three runs per condition and a deterministic source. The hidden workload may combine conditions we did not test."),
              bullet("<b>Platform dependence.</b> Both control loops read Linux TCP_INFO. On other platforms, the network gate is off and a ping still in flight counts as a stall."),
              bullet("<b>Congestion-resilient feedback.</b> Future work could use client reading-progress reports as a pause signal that remains reliable under congestion."),
              bullet("<b>Lower probing overhead.</b> Ping less often once a client has proven fast."),
              bullet("<b>Speech-recognition integration.</b> Connect a real lecture speech recognizer through the same protocol.")]

    doc.build(story, onFirstPage=lambda c, d: footer(c, d, preview), onLaterPages=lambda c, d: footer(c, d, preview))
    print(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preview", action="store_true", help="allow missing T1-T5 data and watermark the PDF")
    parser.add_argument("--font-dir", help="directory containing the four Times New Roman TTF files")
    parser.add_argument("--output", help="output PDF path (default: report.pdf in the source root)")
    args = parser.parse_args()
    register_times(args.font_dir)
    STYLES = styles()
    build(args.preview, args.output)
