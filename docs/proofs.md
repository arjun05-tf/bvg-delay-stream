# Proofs

Three claims, three scripted procedures, and a place to write down what the
run actually produced. The numbers below are blank until the run happens; a
number in this file that was not measured would make every other number in
the repository worthless.

Each proof needs recorded traffic. The producer records every snapshot to the
`snapshots` volume, so leave the stack running for a day before starting:

```bash
make up            # then wait, 24 hours gives 2880 snapshots at 30 s
docker compose exec producer sh -c 'ls /snapshots | wc -l'
```

---

## Proof 1: replay from offset 0 reproduces the output

**Claim.** Reading the same 24 hours of input a second time produces an output
table identical to the first run, row for row and value for value.

**Procedure.**

```bash
make proof-replay          # checksums the current output, truncates it,
                           # restarts the job from offset 0 under a new group
# wait until the Flink UI shows the backlog consumed
make proof-replay-verify   # checksums again and compares
```

**What the comparison covers.** A content checksum of
`delay_tumbling_1h`, `delay_sliding_1h_5m` and `late_events`, computed over
every column except `updated_at` and `recorded_at`, ordered so the checksum
does not depend on row order. `updated_at` is wall clock and must differ.

**Result.**

| Table | Rows, first run | Rows, replay | Checksum match |
| --- | --- | --- | --- |
| `delay_tumbling_1h` | | | |
| `delay_sliding_1h_5m` | | | |
| `late_events` | | | |

Replay wall time: _to fill in_.
Notes: _to fill in_.

---

## Proof 2: kill the job mid-stream, recover from checkpoint

**Claim.** Killing the processing job and restarting it from its last
checkpoint leaves no gap and no duplicate in the sink.

**Procedure.**

```bash
make proof-checkpoint          # records row counts, SIGKILLs the task manager
docker compose logs -f jobmanager | grep -i restor   # watch the restore
make proof-checkpoint-verify   # duplicate check and hour-by-hour coverage
```

**Why no duplicates are possible.** The primary key of each aggregate table is
the window identity. A replayed window overwrites itself. The duplicate check
in `sql/checks/recovery_checks.sql` proves the constraint holds rather than
assuming it.

**Why no gap is possible.** Kafka offsets live in the checkpoint, not in the
broker's committed offsets, so recovery rereads everything after the last
checkpoint. At a 30 second checkpoint interval, recovery reprocesses at most
30 seconds of input.

**Result.**

| Measurement | Value |
| --- | --- |
| Rows before the kill | |
| Duplicate window keys after recovery | |
| Hours covered before / after | |
| Time from kill to first post-recovery checkpoint | |
| Records reprocessed (lag spike area) | |

Notes: _to fill in_.

A note on scope: this kills the task manager, which is the processing job. The
job manager runs without high availability, so killing *it* needs the manual
restore in [recovery-semantics.md](recovery-semantics.md). That gap is listed
in the known limitations rather than papered over.

---

## Proof 3: load test at ten times the natural rate

**Claim.** The pipeline sustains ten times the feed's natural message rate,
and when it eventually cannot, the first thing to degrade is known and
explained.

**Procedure.**

```bash
SPEEDUP=10 LOOPS=1 make proof-load
```

The load generator replays recorded snapshots, so the key distribution,
message sizes and delay distribution are the real ones rather than a tidy
synthetic approximation. The lag probe samples committed offsets against high
watermarks every ten seconds into `consumer_lag_samples`.

**Result.**

| Measurement | Value |
| --- | --- |
| Natural rate (messages/s) | |
| Sustained rate at 10x (messages/s) | |
| Peak consumer lag (messages) | |
| Time to drain the lag after the test | |
| p50 / p99 end-to-end latency during the test | |
| Late-record rate during the test | |

**What degraded first, and why.** _to fill in after the run_. Record the
answer honestly, with the evidence: which Flink metric moved first, whether
backpressure appeared at the sink or at the window operator, and what the
memory and CPU of each container looked like
(`docker stats --no-stream`).

The three candidates worth checking, in the order they are most likely:

1. **The sink.** Batched upserts into a single Postgres connection per
   subtask. At high rates this is the narrowest part of the pipeline. Signal:
   backpressure at the sink operator in the Flink UI, lag climbing steadily,
   CPU on the TimescaleDB container at its limit.
2. **The sliding window state.** Twelve overlapping panes per key means
   twelve times the state of the tumbling window. Signal: task manager
   managed memory pressure, checkpoint duration growing, then checkpoint
   timeouts.
3. **The dimension lookup.** Cold cache on a new deployment issues one query
   per unseen key. Signal: a lag spike in the first minute that clears on its
   own as the cache fills.
