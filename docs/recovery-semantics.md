# Recovery semantics

What this pipeline guarantees, what it does not, and why.

## Summary

| Boundary | Guarantee | Mechanism |
| --- | --- | --- |
| Feed to Kafka | at least once | idempotent producer, retries, no acknowledgement means no progress |
| Kafka to Flink state | exactly once | aligned checkpoints, offsets stored in the checkpoint |
| Flink state to TimescaleDB | effectively exactly once | idempotent upsert keyed on window identity |
| Flink to dead letter topic | at least once | a duplicate dead letter is harmless; a lost one is not |

"Effectively exactly once" is the accurate phrase for the sink. The write path
is not transactional with the checkpoint. Rows can be written twice. Both
writes store the same values, so the observable result is the same as if each
row had been written once.

## Checkpoints

| Setting | Value | Reason |
| --- | --- | --- |
| Interval | 30 s | recovery replays at most 30 s of input |
| Mode | aligned, exactly once | unaligned trades duplicate-free state for latency under backpressure; this job has the headroom |
| Min pause | 10 s | a slow checkpoint cannot starve processing |
| Timeout | 2 min | long enough for a sink flush under load, short enough to notice a stuck one |
| Tolerable failures | 3 | a few failures are a slow sink, not a broken job |
| Retention | retain on cancellation | a stopped job can be resumed by hand |
| State backend | hashmap, filesystem checkpoints | the keyed state is small; RocksDB would add disk and memory for no benefit at this volume |
| Restart strategy | fixed delay, 5 attempts, 10 s | bounded, so a permanently broken job fails instead of looping forever |

Checkpoints are written to a volume shared by the job manager and the task
manager at `/flink-checkpoints`.

## What happens on each failure

### The task manager dies

The job manager notices the lost slots, the job restarts under the fixed
delay strategy, and the operators restore from the last completed checkpoint.
Kafka offsets come from that checkpoint, not from the broker's committed
offsets, so the records that followed it are read again. Those records are
reprocessed into the same windows and upserted over the same rows.

Visible effects: a gap of a few seconds in processing, no gap in the output,
no duplicate rows. The second half of `make proof-checkpoint` checks exactly
that.

### The job manager dies

This deployment runs a single job manager with no high availability, which is
the right call for a laptop and the wrong call for production. Without HA
there is no leader election and no persisted job graph, so a restarted job
manager starts the job from scratch rather than from the last checkpoint.

The recovery path is manual and documented rather than automatic:

```bash
# Find the newest retained checkpoint.
docker compose exec jobmanager \
  find /flink-checkpoints -name '_metadata' -printf '%T@ %p\n' | sort -n | tail -1

# Restart the job from it.
docker compose stop jobmanager taskmanager
FLINK_PROPERTIES_EXTRA="execution.savepoint.path: /flink-checkpoints/<job-id>/chk-<n>" \
  docker compose up -d jobmanager taskmanager
```

Adding ZooKeeper or Kubernetes HA would make this automatic and would cost
another container and another few hundred megabytes. The trade is deliberate
and it is in the known limitations.

### The sink is unavailable

The flush fails, the sink rolls back and rethrows, and the job fails and
restarts from the last checkpoint. The sink itself contains no retry loop.
That is deliberate: Flink already owns retry and recovery, a second retry loop
inside the sink would hide the failure from the checkpoint metrics, and a job
that keeps running while it cannot write looks healthy and is not.

### The producer dies

The producer holds no state worth keeping. On restart it fetches the current
snapshot and carries on. Up to one poll interval of observations can be
missed, which for a FULL_DATASET feed usually heals by itself: the next
snapshot still contains the stop events whose times fall inside the
`MAX_EVENT_AGE_SECONDS` horizon. A longer outage than that horizon is a real
gap, and the dashboard shows it as a drop in event count.

### A poisoned message

A record that cannot be decoded never reaches the windows. It goes to a side
output and then to `stop-events.dlq` with the reason and the source
coordinates in headers. The job does not stop, and nothing is dropped
silently. The same applies in the producer: a snapshot that is not a valid
FeedMessage, and individual stop time updates that are structurally unusable,
are written to the same topic.

## Why replay reproduces the same output

Four properties together:

1. **Deterministic input.** The source is a Kafka topic read from offset 0.
   Keys are stable, so the partition assignment of a given record is the same
   in both runs.
2. **Deterministic event time.** Event time comes from the message payload,
   never from the clock.
3. **Deterministic watermarks.** Bounded out-of-orderness is a function of the
   observed timestamps alone. Source idleness, which is a function of wall
   clock, is off by default. This is the one setting that breaks the property
   if changed.
4. **Idempotent output.** Every write is an upsert keyed on the window
   identity, so the row count and the row values converge regardless of how
   often a window is emitted.

What is *not* identical between two runs: `updated_at`, and the rows in
`e2e_latency_samples`. Both are wall clock by definition. The verification
query excludes them, which is documented in `tools/verify_replay.py` rather
than hidden.

The property is weaker if `JOB_PARALLELISM` changes between the runs: windows
would be assigned to different subtasks, and the per-key results would still
match, but the order of writes within a batch would differ. Content is
compared, not write order, so the check still passes. Changing the window
size or the allowed lateness between runs legitimately changes the output,
and the comparison will say so.
