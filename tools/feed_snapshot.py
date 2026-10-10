"""Summarise one live snapshot of the VBB GTFS-Realtime feed.

Writes `.github/assets/snapshot.json`, which the README asset generator reads.
It is kept as a committed file so the visuals can be rebuilt without the
network, and so the numbers in the README name the snapshot they came from.

    python tools/feed_snapshot.py
"""
import json
import pathlib
import time
import urllib.request

from google.transit import gtfs_realtime_pb2

FEED_URL = "https://production.gtfsrt.vbb.de/data"
BERLIN_PREFIX = "de:11000:"
OUT = pathlib.Path(__file__).parent.parent / ".github" / "assets" / "snapshot.json"

# Seconds. Upper edge of each bucket; the last one catches everything above.
BUCKETS = [
    ("early, over 1 min", -61),
    ("early, under 1 min", -1),
    ("exactly on time", 0),
    ("late, under 1 min", 60),
    ("1 to 3 min", 180),
    ("3 to 5 min", 300),
    ("5 to 10 min", 600),
    ("over 10 min", None),
]


def main():
    started = time.time()
    raw = urllib.request.urlopen(FEED_URL, timeout=120).read()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    stop_time_updates = berlin = 0
    delays = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        for update in entity.trip_update.stop_time_update:
            stop_time_updates += 1
            if update.stop_id.startswith(BERLIN_PREFIX):
                berlin += 1
            event = (
                update.departure
                if update.HasField("departure")
                else update.arrival if update.HasField("arrival") else None
            )
            if event is not None and event.HasField("delay"):
                delays.append(event.delay)

    histogram, lower = [], None
    for label, upper in BUCKETS:
        count = sum(
            1
            for d in delays
            if (lower is None or d > lower) and (upper is None or d <= upper)
        )
        histogram.append({"label": label, "count": count})
        lower = upper

    delays.sort()

    def percentile(p):
        return delays[min(len(delays) - 1, int(len(delays) * p / 100))]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps(
            {
                "feed_url": FEED_URL,
                "header_timestamp": feed.header.timestamp,
                "fetched_utc": time.strftime(
                    "%Y-%m-%d %H:%M UTC", time.gmtime(feed.header.timestamp)
                ),
                "fetch_seconds": round(time.time() - started, 2),
                "bytes": len(raw),
                "trip_updates": len(feed.entity),
                "stop_time_updates": stop_time_updates,
                "berlin_stop_time_updates": berlin,
                "with_delay": len(delays),
                "histogram": histogram,
                "percentiles": {f"p{p}": percentile(p) for p in (5, 50, 90, 95, 99)},
                "on_time_within_3_min": round(
                    100 * sum(1 for d in delays if abs(d) <= 180) / len(delays), 1
                ),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"{OUT.name}: {len(feed.entity)} trip updates, {stop_time_updates} stop time "
          f"updates, {len(delays)} with a delay")


if __name__ == "__main__":
    main()
