-- Rebuild `silver.zone_demand_5min` from `silver.trip_events`.
--
-- The stream recomputes only the windows a batch touched (streaming/silver.py). This repairs the
-- whole table in one shot: a partial restore, a hand-truncated table, or a `trip_id` that changed
-- zone/time after it was stored. Stop the job first: it is safe with the job up (the advisory lock
-- below serializes with the stream), but a full rebuild while the stream runs only makes both wait.
--
-- Idempotent: the table is rebuilt from the source, so running it twice gives the same rows.
BEGIN;

-- The same advisory lock the stream takes before recomputing a window: the backfill serializes with
-- a running job instead of racing its upsert (a DELETE holding rows past the job's 30 s lock_timeout
-- would abort the batch, and the orders of the two writers could deadlock).
SELECT pg_advisory_xact_lock(hashtext('silver.zone_demand_5min'));

DELETE FROM silver.zone_demand_5min;

INSERT INTO silver.zone_demand_5min
    (pickup_zone_id, window_start, window_end, trips, revenue, avg_duration_min)
SELECT
    pickup_zone_id,
    date_bin('5 minutes', pickup_at, TIMESTAMP '2000-01-01 00:00:00'),
    date_bin('5 minutes', pickup_at, TIMESTAMP '2000-01-01 00:00:00') + interval '5 minutes',
    count(*),
    COALESCE(sum(total_amount), 0),
    avg(extract(epoch FROM (dropoff_at - pickup_at)) / 60.0)
        FILTER (WHERE dropoff_at IS NOT NULL)
FROM silver.trip_events
WHERE is_valid_trip
GROUP BY 1, 2;

COMMIT;
