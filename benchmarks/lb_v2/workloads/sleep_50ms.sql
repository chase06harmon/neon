-- Each transaction holds the backend conn for ~50ms. Used as the
-- steady-state workload so backend conns don't churn, making the
-- per-config conn-count differences obvious in the metrics.
SELECT pg_sleep(0.05);
