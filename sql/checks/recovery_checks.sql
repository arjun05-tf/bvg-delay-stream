-- Checks run after a checkpoint recovery. See docs/proofs.md.

-- Duplicates are impossible by construction: the primary key is the window
-- identity, so a replayed window overwrites itself. This query proves the
-- constraint is doing its job rather than assuming it.
SELECT 'duplicate window keys' AS check_name, count(*) AS violations
FROM (
    SELECT window_start, route_id, stop_id
    FROM delay_tumbling_1h
    GROUP BY 1, 2, 3
    HAVING count(*) > 1
) AS duplicates;

-- Coverage. A gap caused by a lost checkpoint shows up as a missing hour in
-- an otherwise continuous series.
SELECT 'hours covered' AS check_name,
       count(DISTINCT window_start) AS windows,
       min(window_start) AS first_window,
       max(window_start) AS last_window
FROM delay_tumbling_1h;

-- The same check, per hour, so a gap is visible rather than averaged away.
SELECT window_start,
       count(*) AS series,
       sum(event_count) AS events
FROM delay_tumbling_1h
GROUP BY window_start
ORDER BY window_start;
