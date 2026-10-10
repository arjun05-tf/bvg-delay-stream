"""Generate the animated SVGs used by README.md.

Everything numeric here comes from `snapshot.json`, which is one real snapshot
of the VBB GTFS-Realtime feed written by `python tools/feed_snapshot.py`. The
dots streaming across the hero are drawn in the proportions of that snapshot's
delay histogram, so the colour mix on screen is the punctuality mix in the
feed.

    python .github/assets/make_assets.py
"""
import json
import pathlib
import random

OUT = pathlib.Path(__file__).parent
SNAPSHOT = json.loads((OUT / "snapshot.json").read_text(encoding="utf-8"))

BG, PANEL, INK, MUTED, DIM = "#0b0f14", "#1b2735", "#e6edf3", "#8b98a6", "#5a6673"
GOLD, RED, CYAN, GREEN, VIOLET = "#f2b53c", "#e0564f", "#4cc9f0", "#4ade80", "#a78bfa"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
SANS = "-apple-system, BlinkMacSystemFont, Segoe UI, Inter, Helvetica, sans-serif"

# Early, on time, a little late, properly late. The hero colours the stream with
# these and the bar chart reuses them, so one legend covers both.
BUCKET_COLORS = [CYAN, CYAN, GREEN, GREEN, GOLD, GOLD, "#fb923c", RED]

PARTITIONS = 6  # stop-events, as created by the compose stack


def trim(value, decimals=1):
    text = f"{value:.{decimals}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def panel(width, height, title=None, subtitle=None):
    parts = [
        f'<rect width="{width}" height="{height}" rx="14" fill="{BG}"/>',
        f'<rect width="{width}" height="{height}" rx="14" fill="url(#grid)"/>',
        f'<rect x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="14" '
        f'fill="none" stroke="{PANEL}"/>',
    ]
    if title:
        parts.append(
            f'<text x="28" y="36" font-family="{MONO}" font-size="12" fill="{MUTED}" '
            f'letter-spacing="2.4">{title}</text>'
        )
    if subtitle:
        parts.append(
            f'<text x="28" y="58" font-family="{SANS}" font-size="13" fill="{DIM}">{subtitle}</text>'
        )
    return "".join(parts)


def svg(width, height, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img"><defs>'
        f'<pattern id="grid" width="36" height="36" patternUnits="userSpaceOnUse">'
        f'<path d="M36 0H0V36" fill="none" stroke="#131c26" stroke-width="1"/></pattern>'
        f'<linearGradient id="goldfill" x1="0" y1="0" x2="1" y2="0">'
        f'<stop offset="0%" stop-color="{GOLD}"/><stop offset="50%" stop-color="#fff0c0"/>'
        f'<stop offset="100%" stop-color="{GOLD}"/>'
        f'<animateTransform attributeName="gradientTransform" type="translate" from="-1 0" '
        f'to="1 0" dur="7s" repeatCount="indefinite"/></linearGradient>'
        f'<linearGradient id="lanefade" x1="0" y1="0" x2="1" y2="0">'
        f'<stop offset="0%" stop-color="{PANEL}" stop-opacity="0.15"/>'
        f'<stop offset="50%" stop-color="{PANEL}" stop-opacity="1"/>'
        f'<stop offset="100%" stop-color="{PANEL}" stop-opacity="0.15"/>'
        f'</linearGradient></defs>{body}</svg>'
    )


def bucket_sampler(rng):
    """Draw a delay bucket in the proportions the live snapshot measured."""
    counts = [b["count"] for b in SNAPSHOT["histogram"]]
    total = sum(counts)
    cumulative, running = [], 0
    for count in counts:
        running += count
        cumulative.append(running / total)

    def draw():
        roll = rng.random()
        for i, edge in enumerate(cumulative):
            if roll <= edge:
                return i
        return len(cumulative) - 1

    return draw


# ---------------------------------------------------------------- hero


def hero():
    width, height = 1200, 430
    lane_x0, lane_x1 = 700, 1142
    top, lane_gap = 96, 34
    rng = random.Random(7)
    draw_bucket = bucket_sampler(rng)

    body = [panel(width, height)]
    body.append(
        f'<clipPath id="streamclip"><rect x="628" y="14" width="{width - 642}" '
        f'height="{height - 56}" rx="12"/></clipPath><g clip-path="url(#streamclip)">'
    )

    # The source rail and the partition lanes it feeds.
    body.append(
        f'<rect x="{lane_x0 - 36}" y="{top - 16}" width="4" height="{lane_gap * PARTITIONS}" '
        f'rx="2" fill="{VIOLET}" fill-opacity="0.5"/>'
        f'<text x="{lane_x0 - 44}" y="{top + lane_gap * PARTITIONS / 2 - 16}" '
        f'text-anchor="end" font-family="{MONO}" font-size="10" fill="{DIM}" '
        f'transform="rotate(-90 {lane_x0 - 44} {top + lane_gap * PARTITIONS / 2 - 16})">'
        f'VBB feed</text>'
    )
    for lane in range(PARTITIONS):
        y = top + lane * lane_gap
        body.append(
            f'<line x1="{lane_x0 - 30}" y1="{y}" x2="{lane_x1}" y2="{y}" '
            f'stroke="url(#lanefade)" stroke-width="1"/>'
            f'<text x="{lane_x1 + 8}" y="{y + 4}" font-family="{MONO}" font-size="9" '
            f'fill="{DIM}">p{lane}</text>'
        )

    # One dot is one StopTimeUpdate. Thirteen per lane keeps the stream dense
    # without turning the file into a flipbook.
    for lane in range(PARTITIONS):
        y = top + lane * lane_gap
        for n in range(13):
            bucket = draw_bucket()
            colour = BUCKET_COLORS[bucket]
            duration = rng.uniform(5.5, 8.5)
            begin = -rng.uniform(0, duration)
            radius = 2.0 if bucket < 4 else 3.0
            body.append(
                f'<circle cy="{y}" r="{radius}" fill="{colour}" fill-opacity="0.9">'
                f'<animate attributeName="cx" from="{lane_x0 - 30}" to="{lane_x1}" '
                f'dur="{trim(duration, 2)}s" begin="{trim(begin, 2)}s" '
                f'repeatCount="indefinite"/>'
                f'<animate attributeName="fill-opacity" values="0;0.95;0.95;0" '
                f'keyTimes="0;0.06;0.9;1" dur="{trim(duration, 2)}s" '
                f'begin="{trim(begin, 2)}s" repeatCount="indefinite"/></circle>'
            )

    body.append("</g>")
    snapshot_line = (
        f'{SNAPSHOT["stop_time_updates"]:,} StopTimeUpdates in one 30 s snapshot '
        f'&#183; colour = reported delay &#183; {SNAPSHOT["fetched_utc"]}'
    )
    body.append(
        f'<text x="{(lane_x0 + lane_x1) / 2 - 20}" y="{height - 26}" text-anchor="middle" '
        f'font-family="{MONO}" font-size="10.5" fill="{DIM}">{snapshot_line}</text>'
    )
    legend_x = 700
    for label, colour in (("early", CYAN), ("on time", GREEN), ("1 to 5 min", GOLD),
                          ("over 5 min", RED)):
        body.append(
            f'<circle cx="{legend_x}" cy="{height - 50}" r="4" fill="{colour}"/>'
            f'<text x="{legend_x + 10}" y="{height - 46}" font-family="{MONO}" font-size="9.5" '
            f'fill="{DIM}">{label}</text>'
        )
        legend_x += 30 + len(label) * 6.2

    body.append(
        f'<text x="60" y="92" font-family="{MONO}" font-size="12" fill="{CYAN}" '
        f'letter-spacing="3.4">EVENT TIME &#183; PROTOBUF &#183; WATERMARKS</text>'
    )
    body.append(
        f'<text x="60" y="152" font-family="{MONO}" font-size="42" font-weight="700" '
        f'fill="url(#goldfill)">bvg-delay-stream</text>'
    )
    body.append(
        f'<line x1="60" y1="176" x2="60" y2="176" stroke="{GOLD}" stroke-width="2" '
        f'stroke-opacity="0.55"><animate attributeName="x2" from="60" to="520" dur="1.4s" '
        f'fill="freeze"/></line>'
    )
    body.append(
        f'<text x="60" y="206" font-family="{SANS}" font-size="16" fill="{MUTED}">'
        f'Berlin transit punctuality, computed on event time, on one laptop.</text>'
    )

    query = "make up   # Kafka, Flink, Timescale and Grafana, all local"
    answer = "delay_tumbling_1h filling &#183; watermark at 20:00"
    qw, steps = 448, 32
    tick_times = ";".join(f"{i / 64:.4f}" for i in range(steps))
    widths = ";".join(f"{qw * min(i, len(query)) / len(query):.1f}" for i in range(steps))
    carets = ";".join(f"{94 + qw * min(i, len(query)) / len(query):.1f}" for i in range(steps))
    body.append(
        f'<rect x="60" y="238" width="524" height="94" rx="10" fill="#0e151d" stroke="{PANEL}"/>'
        f'<text x="76" y="270" font-family="{MONO}" font-size="13" fill="{GOLD}">$</text>'
        f'<clipPath id="typeclip"><rect x="94" y="254" height="22" width="0">'
        f'<animate attributeName="width" dur="9s" repeatCount="indefinite" calcMode="discrete" '
        f'keyTimes="{tick_times}" values="{widths}"/></rect></clipPath>'
        f'<g clip-path="url(#typeclip)"><text x="94" y="270" font-family="{MONO}" font-size="12" '
        f'fill="{INK}" textLength="{qw}" lengthAdjust="spacingAndGlyphs">{query}</text></g>'
        f'<rect y="256" width="7" height="16" fill="{GOLD}">'
        f'<animate attributeName="x" dur="9s" repeatCount="indefinite" calcMode="discrete" '
        f'keyTimes="{tick_times}" values="{carets}"/>'
        f'<animate attributeName="opacity" values="1;0;1" dur="0.9s" repeatCount="indefinite"/>'
        f'</rect>'
        f'<text x="94" y="308" font-family="{MONO}" font-size="13" fill="{CYAN}" '
        f'textLength="340" lengthAdjust="spacingAndGlyphs" opacity="0">{answer}'
        f'<animate attributeName="opacity" dur="9s" repeatCount="indefinite" '
        f'keyTimes="0;0.52;0.6;0.93;1" values="0;0;1;1;0"/></text>'
    )

    coverage = 100 * SNAPSHOT["with_delay"] / SNAPSHOT["stop_time_updates"]
    chips = [
        (f'{SNAPSHOT["trip_updates"]:,}', "trip updates"),
        (f'{coverage:.0f}%', "carry a delay"),
        (f'{SNAPSHOT["on_time_within_3_min"]:.1f}%', "within 3 min"),
        (f'{SNAPSHOT["percentiles"]["p99"] // 60} min', "p99 delay"),
    ]
    x = 60
    for i, (value, label) in enumerate(chips):
        body.append(
            f'<g opacity="0"><animate attributeName="opacity" from="0" to="1" dur="0.5s" '
            f'begin="{1.2 + i * 0.14}s" fill="freeze"/>'
            f'<rect x="{x}" y="356" width="124" height="46" rx="9" fill="#0e151d" '
            f'stroke="{PANEL}"/>'
            f'<text x="{x + 14}" y="379" font-family="{MONO}" font-size="17" font-weight="700" '
            f'fill="{INK}">{value}</text>'
            f'<text x="{x + 14}" y="394" font-family="{MONO}" font-size="9.5" fill="{DIM}" '
            f'letter-spacing="1.1">{label}</text></g>'
        )
        x += 134
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- topology

NODES = [
    ("VBB feed", "GTFS-RT, 30 s", VIOLET),
    ("Producer", "protobuf, registry", CYAN),
    ("stop-events", f"{PARTITIONS} partitions", GOLD),
    ("Flink 1.20", "event-time windows", GREEN),
    ("TimescaleDB", "idempotent upsert", "#fb923c"),
    ("Grafana", "line, station, hour", RED),
]


def topology():
    width, height = 1200, 290
    box_w, gap, y, cycle = 164, 26, 108, 8.0
    total = len(NODES) * box_w + (len(NODES) - 1) * gap
    x0 = (width - total) / 2

    body = [
        panel(
            width,
            height,
            "TOPOLOGY",
            "one process per box, all of it under docker compose on 6 GB",
        )
    ]
    body.append(
        f'<line x1="{x0}" y1="{y + 40}" x2="{x0 + total}" y2="{y + 40}" stroke="{PANEL}" '
        f'stroke-width="2"/>'
    )
    for i, (name, detail, colour) in enumerate(NODES):
        x = x0 + i * (box_w + gap)
        begin = f"{i * cycle / len(NODES):.2f}s"
        body.append(
            f'<rect x="{x}" y="{y}" width="{box_w}" height="80" rx="11" fill="#0e151d" '
            f'stroke="{PANEL}"><animate attributeName="stroke" values="{PANEL};{colour};{PANEL}" '
            f'dur="{cycle}s" begin="{begin}" repeatCount="indefinite" keyTimes="0;0.08;0.3"/>'
            f'</rect>'
            f'<rect x="{x}" y="{y}" width="{box_w}" height="80" rx="11" fill="{colour}" '
            f'opacity="0"><animate attributeName="opacity" values="0;0.11;0" dur="{cycle}s" '
            f'begin="{begin}" repeatCount="indefinite" keyTimes="0;0.08;0.3"/></rect>'
            f'<text x="{x + 16}" y="{y + 26}" font-family="{MONO}" font-size="11" fill="{colour}" '
            f'letter-spacing="1.6">{i + 1:02d}</text>'
            f'<text x="{x + 16}" y="{y + 48}" font-family="{SANS}" font-size="15" '
            f'font-weight="600" fill="{INK}">{name}</text>'
            f'<text x="{x + 16}" y="{y + 66}" font-family="{MONO}" font-size="10" '
            f'fill="{MUTED}">{detail}</text>'
        )
        if i < len(NODES) - 1:
            mid = x + box_w + gap / 2
            body.append(
                f'<path d="M{mid - 6} {y + 34} l6 6 -6 6" fill="none" stroke="{DIM}" '
                f'stroke-width="1.6"/>'
            )
    body.append(
        f'<circle cy="{y + 40}" r="4.5" fill="{GOLD}"><animate attributeName="cx" from="{x0}" '
        f'to="{x0 + total}" dur="{cycle}s" repeatCount="indefinite"/>'
        f'<animate attributeName="opacity" values="0;1;1;0" keyTimes="0;0.04;0.96;1" '
        f'dur="{cycle}s" repeatCount="indefinite"/></circle>'
    )

    # The two escape hatches, drawn below the happy path because that is what
    # they are: anything that cannot be decoded or arrives too late still lands
    # somewhere durable.
    dlq_x = x0 + 2 * (box_w + gap)
    late_x = x0 + 4 * (box_w + gap)
    for x, label, detail, colour in (
        (dlq_x, "stop-events.dlq", "undecodable, from both sides", RED),
        (late_x, "late_events", "past the watermark, side output", "#fb923c"),
    ):
        body.append(
            f'<path d="M{x + box_w / 2} {y + 80} v26" stroke="{colour}" stroke-width="1.4" '
            f'stroke-dasharray="4 4" fill="none" stroke-opacity="0.7"/>'
            f'<rect x="{x - 16}" y="{y + 106}" width="{box_w + 32}" height="44" rx="9" '
            f'fill="#0e151d" stroke="{PANEL}"/>'
            f'<text x="{x}" y="{y + 126}" font-family="{MONO}" font-size="11.5" fill="{colour}">'
            f'{label}</text>'
            f'<text x="{x}" y="{y + 142}" font-family="{MONO}" font-size="9.5" fill="{DIM}">'
            f'{detail}</text>'
        )
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- delays


def delays():
    width, height = 1200, 400
    left, top, col_h = 96, 108, 160
    histogram = SNAPSHOT["histogram"]
    total = sum(b["count"] for b in histogram)
    all_events = SNAPSHOT["stop_time_updates"]
    peak = max(b["count"] for b in histogram)
    gap = (width - left - 80) / len(histogram)
    bar_w = gap - 26
    baseline = top + col_h

    body = [
        panel(
            width,
            height,
            "REPORTED DELAY &#183; ONE LIVE SNAPSHOT",
            f'{total:,} of {all_events:,} stop events carry a delay, the rest report a '
            f'time only &#183; {SNAPSHOT["fetched_utc"]}',
        )
    ]
    body.append(
        f'<line x1="{left - 20}" y1="{baseline}" x2="{width - 50}" y2="{baseline}" '
        f'stroke="{PANEL}" stroke-width="2"/>'
    )
    for i, bucket in enumerate(histogram):
        x = left + i * gap
        share = 100 * bucket["count"] / total
        # Square root, not linear: 70% of the feed reports exactly zero and a
        # linear axis would flatten every other bucket into the baseline.
        bar_h = col_h * (bucket["count"] / peak) ** 0.5
        colour = BUCKET_COLORS[i]
        body.append(
            f'<rect x="{x:.1f}" y="{baseline}" width="{bar_w:.1f}" height="0" rx="4" '
            f'fill="{colour}" fill-opacity="0.85">'
            f'<animate attributeName="height" from="0" to="{bar_h:.1f}" dur="1.0s" '
            f'begin="{0.3 + i * 0.08}s" fill="freeze" calcMode="spline" '
            f'keySplines="0.2 0.8 0.2 1" keyTimes="0;1"/>'
            f'<animate attributeName="y" from="{baseline}" to="{baseline - bar_h:.1f}" '
            f'dur="1.0s" begin="{0.3 + i * 0.08}s" fill="freeze" calcMode="spline" '
            f'keySplines="0.2 0.8 0.2 1" keyTimes="0;1"/></rect>'
            f'<text x="{x + bar_w / 2:.1f}" y="{baseline - bar_h - 24:.1f}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="13" font-weight="700" fill="{INK}" opacity="0">'
            f'{share:.1f}%'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.4s" '
            f'begin="{1.1 + i * 0.08}s" fill="freeze"/></text>'
            f'<text x="{x + bar_w / 2:.1f}" y="{baseline - bar_h - 10:.1f}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="10" fill="{DIM}" opacity="0">'
            f'{bucket["count"]:,}'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.4s" '
            f'begin="{1.2 + i * 0.08}s" fill="freeze"/></text>'
            f'<text x="{x + bar_w / 2:.1f}" y="{baseline + 24}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="10" fill="{MUTED}">{bucket["label"]}</text>'
        )
    percentiles = SNAPSHOT["percentiles"]
    note = (
        f'Seven in ten of them report no deviation at all, and '
        f'<tspan fill="{GOLD}" font-weight="700">{SNAPSHOT["on_time_within_3_min"]:.1f}%</tspan> '
        f'sits within three minutes, but the tail is long: p95 is '
        f'<tspan fill="{RED}" font-weight="700">{percentiles["p95"] // 60} min</tspan> '
        f'and p99 is <tspan fill="{RED}" font-weight="700">{percentiles["p99"] // 60} min</tspan>.'
    )
    body.append(
        f'<g opacity="0"><animate attributeName="opacity" from="0" to="1" dur="0.6s" begin="2.0s" '
        f'fill="freeze"/>'
        f'<rect x="{left - 20}" y="{baseline + 48}" width="{width - left - 30}" height="40" '
        f'rx="9" fill="#0e151d" stroke="{PANEL}"/>'
        f'<text x="{left - 4}" y="{baseline + 73}" font-family="{SANS}" font-size="13" '
        f'fill="{MUTED}">{note}</text></g>'
    )
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- windows


def windows():
    width, height = 1200, 360
    left, right = 150, 1120
    axis_y, cycle = 250, 12.0
    hours = 5
    hour_w = (right - left) / hours

    body = [
        panel(
            width,
            height,
            "EVENT TIME",
            "tumbling and sliding windows on the same stream, with the watermark that closes them",
        )
    ]
    body.append(
        f'<line x1="{left}" y1="{axis_y}" x2="{right}" y2="{axis_y}" stroke="{PANEL}" '
        f'stroke-width="2"/>'
    )
    for h in range(hours + 1):
        x = left + h * hour_w
        body.append(
            f'<line x1="{x:.1f}" y1="{axis_y - 6}" x2="{x:.1f}" y2="{axis_y + 6}" '
            f'stroke="{DIM}"/>'
            f'<text x="{x:.1f}" y="{axis_y + 24}" text-anchor="middle" font-family="{MONO}" '
            f'font-size="10" fill="{DIM}">{17 + h:02d}:00</text>'
        )
    body.append(
        f'<text x="{(left + right) / 2}" y="{axis_y + 44}" text-anchor="middle" '
        f'font-family="{MONO}" font-size="10" fill="{DIM}">event time, taken from the stop '
        f'time in the feed, not from the clock on the machine</text>'
    )

    # Tumbling: four closed windows above the axis.
    for h in range(hours - 1):
        x = left + h * hour_w
        body.append(
            f'<rect x="{x + 3:.1f}" y="{axis_y - 86}" width="{hour_w - 6:.1f}" height="30" '
            f'rx="6" fill="{GOLD}" fill-opacity="0.12" stroke="{GOLD}" stroke-opacity="0.45"/>'
            f'<text x="{x + hour_w / 2:.1f}" y="{axis_y - 66}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="10.5" fill="{GOLD}">delay_tumbling_1h</text>'
        )
    # Sliding: overlapping windows, offset by the slide, drawn thinner.
    for i in range(7):
        x = left + i * hour_w * 0.5
        body.append(
            f'<rect x="{x + 3:.1f}" y="{axis_y - 140 + (i % 2) * 18}" '
            f'width="{hour_w - 6:.1f}" height="14" rx="5" fill="{CYAN}" fill-opacity="0.14" '
            f'stroke="{CYAN}" stroke-opacity="0.4"/>'
        )
    body.append(
        f'<text x="{left}" y="{axis_y - 150}" font-family="{MONO}" font-size="10.5" '
        f'fill="{CYAN}">delay_sliding_1h_5m, one window every 5 minutes</text>'
    )

    # Events arriving on the axis, then the watermark sweeping past them.
    rng = random.Random(11)
    draw_bucket = bucket_sampler(rng)
    for _ in range(54):
        x = rng.uniform(left + 6, left + hour_w * (hours - 1) - 6)
        bucket = draw_bucket()
        body.append(
            f'<circle cx="{x:.1f}" cy="{axis_y - 16 - rng.uniform(0, 22):.1f}" r="2.6" '
            f'fill="{BUCKET_COLORS[bucket]}" fill-opacity="0.8"/>'
        )

    sweep_to = left + hour_w * (hours - 1)
    body.append(
        f'<g><line y1="{axis_y - 172}" y2="{axis_y + 10}" stroke="{GREEN}" stroke-width="2">'
        f'<animate attributeName="x1" from="{left}" to="{sweep_to:.1f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/>'
        f'<animate attributeName="x2" from="{left}" to="{sweep_to:.1f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/></line>'
        f'<text y="{axis_y - 180}" font-family="{MONO}" font-size="11" fill="{GREEN}" '
        f'text-anchor="middle">watermark'
        f'<animate attributeName="x" from="{left}" to="{sweep_to:.1f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/></text></g>'
    )

    # One late record: it lands behind the watermark and is routed to the side
    # output instead of being dropped, which is the whole reason the tag exists.
    late_x = left + hour_w * 1.4
    body.append(
        f'<g opacity="0"><animate attributeName="opacity" values="0;1;1;0" '
        f'keyTimes="0;0.62;0.95;1" dur="{cycle}s" repeatCount="indefinite"/>'
        f'<circle cx="{late_x:.1f}" cy="{axis_y - 30}" r="5" fill="{RED}"/>'
        f'<path d="M{late_x:.1f} {axis_y - 24} V{axis_y + 60}" stroke="{RED}" '
        f'stroke-width="1.4" stroke-dasharray="4 4" fill="none"/>'
        f'<rect x="{late_x - 62:.1f}" y="{axis_y + 60}" width="150" height="40" rx="9" '
        f'fill="#0e151d" stroke="{RED}" stroke-opacity="0.5"/>'
        f'<text x="{late_x - 48:.1f}" y="{axis_y + 78}" font-family="{MONO}" font-size="11" '
        f'fill="{RED}">late_events</text>'
        f'<text x="{late_x - 48:.1f}" y="{axis_y + 92}" font-family="{MONO}" font-size="9.5" '
        f'fill="{DIM}">side output, not dropped</text></g>'
    )
    return svg(width, height, "".join(body))


if __name__ == "__main__":
    for name, markup in (
        ("hero.svg", hero()),
        ("topology.svg", topology()),
        ("delays.svg", delays()),
        ("windows.svg", windows()),
    ):
        (OUT / name).write_text(markup, encoding="utf-8")
        print(f"{name:16} {len(markup) / 1024:6.1f} KB")
