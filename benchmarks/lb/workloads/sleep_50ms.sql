-- Each transaction holds the backend conn for ~50ms. Used by global_cap
-- to keep many clients pinned to backends, surfacing the cap.
SELECT pg_sleep(0.05);
