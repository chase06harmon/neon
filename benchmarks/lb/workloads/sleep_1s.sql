-- Each transaction holds the backend conn for ~1s. Used by
-- checkout_deadline to drive saturation past the configured timeout.
SELECT pg_sleep(1);
