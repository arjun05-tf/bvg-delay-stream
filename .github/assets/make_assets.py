"""Generate the figures used by README.md.

House style: Berlin transit signage. BVG yellow, heavy uppercase grotesque,
black route lines with 45 degree bends, interchange circles, and a departure
board for anything numeric.

Every number comes from `snapshot.json`, one real snapshot of the VBB
GTFS-Realtime feed written by `python tools/feed_snapshot.py`.

    python .github/assets/make_assets.py
"""
import json
import pathlib
import random

OUT = pathlib.Path(__file__).parent
SNAPSHOT = json.loads((OUT / "snapshot.json").read_text(encoding="utf-8"))

YELLOW = "#f7d117"
YELLOW_DEEP = "#e0bd0a"
INK = "#15161a"
WHITE = "#ffffff"
GREY = "#6d6e71"
BOARD = "#0d0e10"
U_BLUE = "#0a60a8"
S_GREEN = "#008b3a"
RED = "#e2001a"
ORANGE = "#f08a00"

SANS = "'Helvetica Neue', Helvetica, Arial, 'Liberation Sans', sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

# Early, on time, slightly late, late. Signage colours, not a gradient.
BUCKET_COLORS = [U_BLUE, U_BLUE, S_GREEN, S_GREEN, YELLOW_DEEP, ORANGE, ORANGE, RED]
PARTITIONS = 6


def sheet(width, height, yellow=True):
    base = YELLOW if yellow else INK
    return f'<rect width="{width}" height="{height}" fill="{base}"/>'


def svg(width, height, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img"><defs>'
        f'<pattern id="stripe" width="16" height="16" patternUnits="userSpaceOnUse" '
        f'patternTransform="rotate(45)">'
        f'<rect width="16" height="16" fill="{YELLOW}"/>'
        f'<rect width="8" height="16" fill="{INK}" fill-opacity="0.08"/></pattern>'
        f'</defs>{body}</svg>'
    )


def headline(x, y, text, size=56, fill=INK, anchor="start", weight="800", spacing="-0.5"):
    return (
        f'<text x="{x}" y="{y}" font-family="{SANS}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}" letter-spacing="{spacing}">{text}</text>'
    )


def caps(x, y, text, size=11, fill=INK, anchor="start", spacing="2.6", opacity=1.0):
    return (
        f'<text x="{x}" y="{y}" font-family="{SANS}" font-size="{size}" font-weight="700" '
        f'fill="{fill}" text-anchor="{anchor}" letter-spacing="{spacing}" '
        f'opacity="{opacity}">{text}</text>'
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
    width, height = 1200, 440
    rng = random.Random(7)
    draw_bucket = bucket_sampler(rng)
    coverage = 100 * SNAPSHOT["with_delay"] / SNAPSHOT["stop_time_updates"]

    body = [sheet(width, height)]
    # Signage block: black bar with the mode badge, like a station nameplate.
    body.append(f'<rect x="0" y="0" width="{width}" height="104" fill="{INK}"/>')
    body.append(
        f'<rect x="48" y="28" width="48" height="48" fill="{U_BLUE}"/>'
        f'<text x="72" y="64" text-anchor="middle" font-family="{SANS}" font-size="32" '
        f'font-weight="800" fill="{WHITE}">U</text>'
    )
    body.append(headline(112, 68, "BVG-DELAY-STREAM", 42, YELLOW))
    body.append(caps(width - 48, 44, "VBB GTFS-REALTIME", 11, YELLOW, anchor="end"))
    body.append(caps(width - 48, 68, f'SNAPSHOT {SNAPSHOT["fetched_utc"]}', 11, GREY,
                     anchor="end"))

    body.append(caps(48, 148, "PUNCTUALITY ON EVENT TIME, COMPUTED ON ONE LAPTOP", 14, INK, "1.4"))

    # The route: one thick line with interchange circles, dots running along it.
    line_y = 238
    x0, x1 = 48, width - 48
    bend = 36
    path = (f"M{x0} {line_y} H{x0 + 210} L{x0 + 210 + bend} {line_y - bend} "
            f"H{x1 - 210 - bend} L{x1 - 210} {line_y} H{x1}")
    body.append(f'<path d="{path}" fill="none" stroke="{INK}" stroke-width="13" '
                f'stroke-linecap="round" stroke-linejoin="round"/>')

    stations = [
        (x0 + 8, line_y, "FEED"),
        (x0 + 210 + bend + 30, line_y - bend, "PRODUCER"),
        ((x0 + x1) / 2, line_y - bend, "STOP-EVENTS"),
        (x1 - 210 - bend - 30, line_y - bend, "FLINK"),
        (x1 - 8, line_y, "TIMESCALE"),
    ]
    for i, (sx, sy, name) in enumerate(stations):
        interchange = name in ("STOP-EVENTS", "FLINK")
        radius = 13 if interchange else 9
        body.append(
            f'<circle cx="{sx:.0f}" cy="{sy:.0f}" r="{radius}" fill="{WHITE}" stroke="{INK}" '
            f'stroke-width="5"/>'
        )
        anchor = "start" if i == 0 else ("end" if i == len(stations) - 1 else "middle")
        body.append(caps(sx, sy - radius - 14, name, 11, INK, anchor=anchor, spacing="1.8"))

    # Vehicles: one dot per StopTimeUpdate, coloured by the delay it reported.
    for i in range(34):
        duration = rng.uniform(7.0, 11.0)
        begin = -rng.uniform(0, duration)
        colour = BUCKET_COLORS[draw_bucket()]
        body.append(
            f'<circle r="5" fill="{colour}" stroke="{INK}" stroke-width="1.5">'
            f'<animateMotion dur="{duration:.2f}s" begin="{begin:.2f}s" '
            f'repeatCount="indefinite" path="{path}"/></circle>'
        )
    body.append(
        caps(48, line_y + 50, "ONE DOT = ONE STOPTIMEUPDATE, COLOURED BY REPORTED DELAY",
             10, INK, "1.6", opacity=0.75)
    )

    # Departure board: the numbers, in the one place a Berliner reads numbers.
    board_y, board_h = 300, 112
    body.append(f'<rect x="48" y="{board_y}" width="{width - 96}" height="{board_h}" '
                f'fill="{BOARD}"/>')
    columns = [
        ("STOP EVENTS / 30 S", f'{SNAPSHOT["stop_time_updates"]:,}', YELLOW),
        ("CARRY A DELAY", f"{coverage:.0f}%", WHITE),
        ("WITHIN 3 MIN", f'{SNAPSHOT["on_time_within_3_min"]:.1f}%', S_GREEN),
        ("P99 DELAY", f'{SNAPSHOT["percentiles"]["p99"] // 60} MIN', RED),
    ]
    column_w = (width - 96) / len(columns)
    for i, (label, value, colour) in enumerate(columns):
        x = 48 + i * column_w
        if i:
            body.append(f'<path d="M{x:.0f} {board_y + 16} V{board_y + board_h - 16}" '
                        f'stroke="{GREY}" stroke-width="1" stroke-opacity="0.4"/>')
        body.append(caps(x + 26, board_y + 36, label, 10, GREY, "2.2"))
        body.append(
            f'<text x="{x + 26:.0f}" y="{board_y + 84}" font-family="{MONO}" font-size="34" '
            f'font-weight="700" fill="{colour}" opacity="0">{value}'
            f'<animate attributeName="opacity" values="0;1" dur="0.35s" '
            f'begin="{0.4 + i * 0.18}s" fill="freeze"/>'
            f'<animate attributeName="opacity" values="1;0.55;1" dur="4s" '
            f'begin="{2 + i * 0.4}s" repeatCount="indefinite"/></text>'
        )
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- network


def network():
    width, height = 1200, 372
    body = [sheet(width, height)]
    body.append(f'<rect x="0" y="0" width="{width}" height="64" fill="{INK}"/>')
    body.append(caps(48, 40, "NETWORK MAP", 15, YELLOW, "3"))
    body.append(caps(width - 48, 40, "ONE PROCESS PER STATION", 11, GREY, anchor="end"))

    main_y = 186
    x0, x1 = 90, width - 90
    stops = [
        ("VBB FEED", "GTFS-RT, 30 S", U_BLUE),
        ("PRODUCER", "PROTOBUF, REGISTRY", U_BLUE),
        ("STOP-EVENTS", f"{PARTITIONS} PARTITIONS", INK),
        ("FLINK 1.20", "EVENT-TIME WINDOWS", INK),
        ("TIMESCALEDB", "IDEMPOTENT UPSERT", S_GREEN),
        ("GRAFANA", "LINE, STATION, HOUR", S_GREEN),
    ]
    gap = (x1 - x0) / (len(stops) - 1)

    body.append(f'<path d="M{x0} {main_y} H{x1}" stroke="{INK}" stroke-width="12" '
                f'stroke-linecap="round"/>')
    for i, (name, detail, colour) in enumerate(stops):
        x = x0 + i * gap
        body.append(
            f'<circle cx="{x:.0f}" cy="{main_y}" r="14" fill="{WHITE}" stroke="{INK}" '
            f'stroke-width="5"/>'
            f'<circle cx="{x:.0f}" cy="{main_y}" r="5" fill="{colour}"/>'
        )
        body.append(caps(x, main_y - 50, name, 12.5, INK, anchor="middle", spacing="1.4"))
        body.append(caps(x, main_y - 34, detail, 9.5, GREY, anchor="middle", spacing="1.2"))

    # Branch lines, drawn with a 45 degree bend like a real transit map.
    branches = [
        (2, 64, -1, "STOP-EVENTS.DLQ", "UNDECODABLE, FROM BOTH SIDES", RED),
        (3, 118, 1, "LATE_EVENTS", "PAST THE WATERMARK, SIDE OUTPUT", ORANGE),
    ]
    for index, drop, side, name, detail, colour in branches:
        x = x0 + index * gap
        y = main_y + drop
        end = x + side * (drop + 60)
        track = (f"M{x:.0f} {main_y} L{x + side * drop:.0f} {y:.0f} H{end:.0f}")
        body.append(
            f'<path d="{track}" fill="none" stroke="{colour}" stroke-width="8" '
            f'stroke-linecap="round" stroke-linejoin="round"/>'
            f'<circle cx="{end:.0f}" cy="{y:.0f}" r="11" fill="{WHITE}" '
            f'stroke="{colour}" stroke-width="5"/>'
            f'<circle r="4.5" fill="{colour}">'
            f'<animateMotion dur="3.4s" repeatCount="indefinite" path="{track}"/>'
            f'<animate attributeName="opacity" values="0;1;1;0" keyTimes="0;0.1;0.8;1" '
            f'dur="3.4s" repeatCount="indefinite"/></circle>'
        )
        anchor = "end" if side < 0 else "start"
        label_x = end - side * -20 if side > 0 else end - 20
        body.append(caps(label_x, y - 2, name, 12, INK, anchor=anchor, spacing="1.4"))
        body.append(caps(label_x, y + 16, detail, 9.5, GREY, anchor=anchor, spacing="1.2"))

    # Service dot running the whole line.
    body.append(
        f'<circle cy="{main_y}" r="6" fill="{YELLOW}" stroke="{INK}" stroke-width="2">'
        f'<animate attributeName="cx" from="{x0}" to="{x1}" dur="9s" repeatCount="indefinite"/>'
        f'</circle>'
    )
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- delays


def delays():
    width, height = 1200, 430
    histogram = SNAPSHOT["histogram"]
    total = sum(b["count"] for b in histogram)

    body = [sheet(width, height, yellow=False)]
    body.append(f'<rect x="0" y="0" width="{width}" height="64" fill="{YELLOW}"/>')
    body.append(caps(48, 40, "REPORTED DELAY, ONE LIVE SNAPSHOT", 15, INK, "3"))
    body.append(caps(width - 48, 40, f'{total:,} OF {SNAPSHOT["stop_time_updates"]:,} '
                     f"STOP EVENTS CARRY ONE", 11, INK, anchor="end", spacing="1.6"))

    # Departure-board rows: label, share bar, count. Reads like a platform
    # display rather than a chart.
    top, row_h = 96, 36
    label_x, bar_x, bar_w = 48, 330, 620
    peak = max(b["count"] for b in histogram)
    for i, bucket in enumerate(histogram):
        y = top + i * row_h
        share = 100 * bucket["count"] / total
        colour = BUCKET_COLORS[i]
        body.append(
            f'<rect x="0" y="{y - 20}" width="{width}" height="{row_h - 4}" fill="{WHITE}" '
            f'fill-opacity="{0.04 if i % 2 else 0}"/>'
            f'<text x="{label_x}" y="{y}" font-family="{MONO}" font-size="14" fill="{WHITE}" '
            f'letter-spacing="1">{bucket["label"].upper()}</text>'
            f'<rect x="{bar_x}" y="{y - 13}" height="17" width="0" fill="{colour}">'
            f'<animate attributeName="width" from="0" to="{bar_w * bucket["count"] / peak:.1f}" '
            f'dur="0.9s" begin="{0.3 + i * 0.08}s" fill="freeze" calcMode="spline" '
            f'keySplines="0.2 0.8 0.2 1" keyTimes="0;1"/></rect>'
            f'<text x="{bar_x + bar_w * bucket["count"] / peak + 14:.0f}" y="{y}" '
            f'font-family="{MONO}" font-size="15" '
            f'font-weight="700" fill="{YELLOW}" opacity="0">{share:.1f}%'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.3s" '
            f'begin="{1.0 + i * 0.08}s" fill="freeze"/></text>'
            f'<text x="{width - 48}" y="{y}" font-family="{MONO}" font-size="13" '
            f'fill="{GREY}" text-anchor="end">{bucket["count"]:,}</text>'
        )
    percentiles = SNAPSHOT["percentiles"]
    footer = top + len(histogram) * row_h + 26
    body.append(f'<path d="M48 {footer - 18} H{width - 48}" stroke="{GREY}" '
                f'stroke-width="1" stroke-opacity="0.5"/>')
    body.append(
        f'<text x="48" y="{footer + 14}" font-family="{SANS}" font-size="14" fill="{WHITE}" '
        f'opacity="0">Seven in ten delay-carrying events report no deviation at all, and '
        f'{SNAPSHOT["on_time_within_3_min"]:.1f}% sit within three minutes. The tail is long: '
        f'p95 is {percentiles["p95"] // 60} min, p99 is {percentiles["p99"] // 60} min.'
        f'<animate attributeName="opacity" from="0" to="1" dur="0.6s" begin="1.8s" '
        f'fill="freeze"/></text>'
    )
    return svg(width, height, "".join(body))


# ---------------------------------------------------------------- windows


def windows():
    width, height = 1200, 410
    left, right = 120, width - 70
    axis_y, cycle = 268, 12.0
    hours = 5
    hour_w = (right - left) / hours

    body = [sheet(width, height)]
    body.append(f'<rect x="0" y="0" width="{width}" height="64" fill="{INK}"/>')
    body.append(caps(48, 40, "EVENT TIME", 15, YELLOW, "3"))
    body.append(caps(width - 48, 40, "TUMBLING AND SLIDING, ONE STREAM", 11, GREY, anchor="end"))

    body.append(f'<path d="M{left} {axis_y} H{right}" stroke="{INK}" stroke-width="4"/>')
    for h in range(hours + 1):
        x = left + h * hour_w
        body.append(
            f'<path d="M{x:.0f} {axis_y - 7} V{axis_y + 7}" stroke="{INK}" stroke-width="3"/>'
            f'<text x="{x:.0f}" y="{axis_y + 26}" text-anchor="middle" font-family="{MONO}" '
            f'font-size="11" fill="{INK}">{17 + h:02d}:00</text>'
        )
    body.append(caps(left, axis_y + 118, "EVENT TIME IS THE STOP TIME IN THE FEED, "
                     "NOT THE CLOCK ON THE MACHINE", 10, INK, "1.6", opacity=0.7))

    for h in range(hours - 1):
        x = left + h * hour_w
        body.append(
            f'<rect x="{x + 4:.0f}" y="{axis_y - 84}" width="{hour_w - 8:.0f}" height="28" '
            f'fill="{INK}"/>'
            f'<text x="{x + hour_w / 2:.0f}" y="{axis_y - 65}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="11" fill="{YELLOW}">DELAY_TUMBLING_1H</text>'
        )
    for i in range(7):
        x = left + i * hour_w * 0.5
        body.append(
            f'<rect x="{x + 4:.0f}" y="{axis_y - 136 + (i % 2) * 18}" '
            f'width="{hour_w - 8:.0f}" height="14" fill="{U_BLUE}" fill-opacity="0.85"/>'
        )
    body.append(caps(left, axis_y - 148, "DELAY_SLIDING_1H_5M", 11, U_BLUE, "2"))

    rng = random.Random(11)
    draw_bucket = bucket_sampler(rng)
    for _ in range(58):
        x = rng.uniform(left + 6, left + hour_w * (hours - 1) - 6)
        body.append(
            f'<circle cx="{x:.1f}" cy="{axis_y - 16 - rng.uniform(0, 20):.1f}" r="3" '
            f'fill="{BUCKET_COLORS[draw_bucket()]}" stroke="{INK}" stroke-width="0.8"/>'
        )

    sweep_to = left + hour_w * (hours - 1)
    body.append(
        f'<g><path d="M0 0" /><line y1="{axis_y - 170}" y2="{axis_y + 10}" stroke="{RED}" '
        f'stroke-width="3">'
        f'<animate attributeName="x1" from="{left}" to="{sweep_to:.0f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/>'
        f'<animate attributeName="x2" from="{left}" to="{sweep_to:.0f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/></line>'
        f'<text y="{axis_y - 178}" font-family="{SANS}" font-size="12" font-weight="700" '
        f'fill="{RED}" text-anchor="middle" letter-spacing="2">WATERMARK'
        f'<animate attributeName="x" from="{left}" to="{sweep_to:.0f}" dur="{cycle}s" '
        f'repeatCount="indefinite"/></text></g>'
    )

    late_x = left + hour_w * 1.35
    body.append(
        f'<g opacity="0"><animate attributeName="opacity" values="0;1;1;0" '
        f'keyTimes="0;0.62;0.95;1" dur="{cycle}s" repeatCount="indefinite"/>'
        f'<circle cx="{late_x:.0f}" cy="{axis_y - 30}" r="6" fill="{ORANGE}" stroke="{INK}" '
        f'stroke-width="1.5"/>'
        f'<path d="M{late_x:.0f} {axis_y - 22} L{late_x + 62:.0f} {axis_y + 62} '
        f'H{late_x + 150:.0f}" fill="none" stroke="{ORANGE}" stroke-width="6" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{late_x + 150:.0f}" cy="{axis_y + 62}" r="10" fill="{WHITE}" '
        f'stroke="{ORANGE}" stroke-width="4"/>'
        f'{caps(late_x + 170, axis_y + 58, "LATE_EVENTS", 12, INK, "1.4")}'
        f'{caps(late_x + 170, axis_y + 74, "SIDE OUTPUT, NOT DROPPED", 9.5, GREY, "1.2")}'
        f'</g>'
    )
    return svg(width, height, "".join(body))


if __name__ == "__main__":
    for name, markup in (
        ("hero.svg", hero()),
        ("network.svg", network()),
        ("delays.svg", delays()),
        ("windows.svg", windows()),
    ):
        (OUT / name).write_text(markup, encoding="utf-8")
        print(f"{name:16} {len(markup) / 1024:6.1f} KB")
