# Proxy

Proxy binary accepts `--auth-backend` CLI option, which determines auth scheme and cluster routing method. Following routing backends are currently implemented:

* console
  new SCRAM-based console API; uses SNI info to select the destination project (endpoint soon)
* postgres
  uses postgres to select auth secrets of existing roles. Useful for local testing
* web (or link)
  sends login link for all usernames

Also proxy can expose following services to the external world:

* postgres protocol over TCP -- usual postgres endpoint compatible with usual
  postgres drivers
* postgres protocol over WebSockets -- same protocol tunneled over websockets
  for environments where TCP connection is not available. We have our own
  implementation of a client that uses node-postgres and tunnels traffic through
  websockets: https://github.com/neondatabase/serverless
* SQL over HTTP -- service that accepts POST requests with SQL text over HTTP
  and responds with JSON-serialised results.


## SQL over HTTP

Contrary to the usual postgres proto over TCP and WebSockets using plain
one-shot HTTP request achieves smaller amortized latencies in edge setups due to
fewer round trips and an enhanced open connection reuse by the v8 engine. Also
such endpoint could be used directly without any driver.

To play with it locally one may start proxy over a local postgres installation
(see end of this page on how to generate certs with openssl):

```
LOGFMT=text ./target/debug/proxy -c server.crt -k server.key --auth-backend=postgres --auth-endpoint=postgres://stas@127.0.0.1:5432/stas --wss 0.0.0.0:4444
```

If both postgres and proxy are running you may send a SQL query:
```console
curl -k -X POST 'https://proxy.local.neon.build:4444/sql' \
  -H 'Neon-Connection-String: postgres://stas:pass@proxy.local.neon.build:4444/postgres' \
  -H 'Content-Type: application/json' \
  --data '{
    "query":"SELECT $1::int[] as arr, $2::jsonb as obj, 42 as num",
    "params":[ "{{1,2},{\"3\",4}}", {"key":"val", "ikey":4242}]
  }' | jq
```
```json
{
  "command": "SELECT",
  "fields": [
    { "dataTypeID": 1007, "name": "arr" },
    { "dataTypeID": 3802, "name": "obj" },
    { "dataTypeID": 23, "name": "num" }
  ],
  "rowCount": 1,
  "rows": [
    {
      "arr": [[1,2],[3,4]],
      "num": 42,
      "obj": {
        "ikey": 4242,
        "key": "val"
      }
    }
  ]
}
```


With the current approach we made the following design decisions:

1. SQL injection protection: We employed the extended query protocol, modifying
   the rust-postgres driver to send queries in one roundtrip using a text
   protocol rather than binary, bypassing potential issues like those identified
   in sfackler/rust-postgres#1030.

2. Postgres type compatibility: As not all postgres types have binary
   representations (e.g., acl's in pg_class), we adjusted rust-postgres to
   respond with text protocol, simplifying serialization and fixing queries with
   text-only types in response.

3. Data type conversion: Considering JSON supports fewer data types than
   Postgres, we perform conversions where possible, passing all other types as
   strings. Key conversions include:
   - postgres int2, int4, float4, float8 -> json number (NaN and Inf remain
     text)
   - postgres bool, null, text -> json bool, null, string
   - postgres array -> json array
   - postgres json and jsonb -> json object

4. Alignment with node-postgres: To facilitate integration with js libraries,
   we've matched the response structure of node-postgres, returning command tags
   and column oids. Command tag capturing was added to the rust-postgres
   functionality as part of this change.

### Output options

User can pass several optional headers that will affect resulting json.

1. `Neon-Raw-Text-Output: true`. Return postgres values as text, without parsing them. So numbers, objects, booleans, nulls and arrays will be returned as text. That can be useful in cases when client code wants to implement it's own parsing or reuse parsing libraries from e.g. node-postgres.
2. `Neon-Array-Mode: true`. Return postgres rows as arrays instead of objects. That is more compact representation and also helps in some edge
cases where it is hard to use rows represented as objects (e.g. when several fields have the same name).

## TCP connection pool

The proxy embeds a TCP/Postgres connection pool keyed by
`(endpoint, database, role)`. With pooling on, an incoming client
session can either reuse a previously opened backend connection or
trigger a new one, subject to per-key, global, and overflow caps.

The pool lives in `proxy/src/tcp_pool.rs`. It enforces a real global
cap on physical compute connections via a process-wide
`tokio::sync::Semaphore`: every backend connection holds a permit for
its entire lifetime, regardless of how many `(endpoint, db, role)`
keys exist. The previous implementation only enforced a per-key cap,
so the effective ceiling was `num_keys × max_conns_per_key`.

### Flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--tcp-pool-enabled` | `false` | Master switch. When off, sessions take the legacy direct-connect path and the global cap does not apply. |
| `--tcp-pool-mode` | `session` | `session` holds a backend conn for the whole client session. `transaction` releases at every `ReadyForQuery('I')` and re-acquires for the next transaction (PgBouncer-style). |
| `--tcp-pool-max-total-conns` | `20000` | Hard ceiling on physical compute connections, enforced via the global semaphore. |
| `--tcp-pool-max-conns-per-key` | `20` | Per-key cap on physical conns (idle + connecting + checked-out). |
| `--tcp-pool-overflow-limit` | `0` | Optional controlled overflow on top of the global cap. Overflow conns are short-lived (never returned to the idle queue). With `0`, saturated pools queue up to the checkout timeout and then return an explicit error. |
| `--tcp-pool-checkout-timeout` | `1s` | Bounded deadline for the entire checkout path (per-key wait + global wait + backend connect). Replaces the previous 365-day timeout. |
| `--tcp-pool-idle-timeout` | `5m` | Reserved for the idle-eviction worker. |
| `--tcp-pool-fallback-direct-connect` | `true` | Legacy flag. The pool no longer bypasses the global cap on saturation; this flag is accepted for backwards compatibility but does not alter pool behavior. Use `--tcp-pool-overflow-limit` for controlled overflow. |

### Metrics

All metrics live under the `proxy_tcp_pool_` prefix and use closed
enum labels (no per-endpoint or per-tenant cardinality):

- `proxy_tcp_pool_connections{state}` — gauge.
  States: `idle`, `checked_out`, `connecting`, `overflow`.
- `proxy_tcp_pool_checkout_total{outcome}` — counter.
  Outcomes: `immediate_hit`, `miss_created`, `queued_hit`,
  `queued_created`, `overflow`, `timeout`, `rejected`, `failed`.
- `proxy_tcp_pool_checkout_wait_seconds{outcome}` — histogram of
  total checkout latency, split by outcome.
- `proxy_tcp_pool_release_total{reason,reusable}` — counter.
  Release reasons include `clean_session_end`,
  `clean_transaction_boundary`, `frontend_terminate`,
  `backend_closed`, `io_error`, `dirty_session_state`,
  `reset_failed`, `idle_eviction`, `global_pressure_eviction`,
  `lifetime_exceeded`. `reusable` is `true`/`false`.
- `proxy_tcp_pool_overflow_connections_total{outcome}` — counter
  (`taken` / `refused`).
- `proxy_tcp_pool_evictions_total{reason}` — counter (registered
  for the upcoming idle-eviction worker; not yet emitted).
- `proxy_tcp_pool_global_pressure` — gauge, `1` if the global
  semaphore is saturated, `0` otherwise.

Endpoint IDs, database names, role names, and compute IDs are kept
internal — they never appear as Prometheus labels.

### Running locally with the pool on

Build with `--features testing` and pass the pool flags. For example:

```sh
RUST_LOG=proxy LOGFMT=text cargo run -p proxy --bin proxy --features testing -- \
  --auth-backend postgres \
  --auth-endpoint 'postgresql://postgres:proxy-postgres@127.0.0.1:5432/postgres' \
  -c server.crt -k server.key \
  --tcp-pool-enabled true \
  --tcp-pool-max-total-conns 16 \
  --tcp-pool-max-conns-per-key 8 \
  --tcp-pool-checkout-timeout 250ms \
  --tcp-pool-overflow-limit 0
```

Hit the proxy with multiple sessions and watch the metrics:

```sh
curl -s http://127.0.0.1:7001/metrics | grep ^proxy_tcp_pool_
```

### Benchmarks

The Phase 1 load-balancing benchmark suite lives in
[`benchmarks/lb/`](../benchmarks/lb/README.md) and currently ships
two scenarios:

- `BENCH=global_cap` — verifies that the peak number of physical
  compute connections is bounded by `--tcp-pool-max-total-conns`
  even when many endpoints together demand more.
- `BENCH=checkout_deadline` — verifies that a saturated pool
  returns explicit `timeout` outcomes instead of blocking forever.

`DRY_RUN=1` validates plumbing without invoking pgbench. Other
scenarios from the brief (`noisy_neighbor`, `transaction_multiplex`,
etc.) are scaffolded but report `not implemented`; they depend on
endpoint-fairness, transaction-mode metrics, and multi-compute work
that is not part of this phase.

## Test proxy locally

For an end-to-end local setup that uses `neon_local`-managed computes
(pageserver + safekeeper + Postgres) instead of a single shared Postgres,
see [`proxy-cplane-api/README.md`](../proxy-cplane-api/README.md).

Proxy determines project name from the subdomain, request to the `round-rice-566201.somedomain.tld` will be routed to the project named `round-rice-566201`. Unfortunately, `/etc/hosts` does not support domain wildcards, so we can use *.local.neon.build` which resolves to `127.0.0.1`.

We will need to have a postgres instance. Assuming that we have set up docker we can set it up as follows:
```sh
docker run \
  --detach \
  --name proxy-postgres \
  --env POSTGRES_PASSWORD=proxy-postgres \
  --publish 5432:5432 \
  postgres:17-bookworm
```

Next step is setting up auth table and schema as well as creating role (without the JWT table):
```sh
docker exec -it proxy-postgres psql -U postgres -c "CREATE SCHEMA IF NOT EXISTS neon_control_plane"
docker exec -it proxy-postgres psql -U postgres -c "CREATE TABLE neon_control_plane.endpoints (endpoint_id VARCHAR(255) PRIMARY KEY, allowed_ips VARCHAR(255))"
docker exec -it proxy-postgres psql -U postgres -c "CREATE ROLE proxy WITH SUPERUSER LOGIN PASSWORD 'password';"
```

If you want to test query cancellation, redis is also required:
```sh
docker run --detach --name proxy-redis --publish 6379:6379 redis:7.0
```

Let's create self-signed certificate by running:
```sh
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.local.neon.build"
```

Then we need to build proxy with 'testing' feature and run, e.g.:
```sh
RUST_LOG=proxy LOGFMT=text cargo run -p proxy --bin proxy --features testing -- \
  --auth-backend postgres --auth-endpoint 'postgresql://postgres:proxy-postgres@127.0.0.1:5432/postgres' \
  --redis-auth-type="plain" --redis-plain="redis://127.0.0.1:6379" \
  -c server.crt -k server.key
```

Now from client you can start a new session:

```sh
PGSSLROOTCERT=./server.crt psql  "postgresql://proxy:password@endpoint.local.neon.build:4432/postgres?sslmode=verify-full"
```

## auth broker setup:

Create a postgres instance:
```sh
docker run \
  --detach \
  --name proxy-postgres \
  --env POSTGRES_HOST_AUTH_METHOD=trust \
  --env POSTGRES_USER=authenticated \
  --env POSTGRES_DB=database \
  --publish 5432:5432 \
  postgres:17-bookworm
```

Create a configuration file called `local_proxy.json` in the root of the repo (used also by the auth broker to validate JWTs)
```sh
{
    "jwks": [
        {
            "id": "1",
            "role_names": ["authenticator", "authenticated", "anon"],
            "jwks_url": "https://climbing-minnow-11.clerk.accounts.dev/.well-known/jwks.json",
            "provider_name": "foo",
            "jwt_audience": null
        }
    ]
}
```

Start the local proxy:
```sh
cargo run --bin local_proxy --features testing -- \
  --disable-pg-session-jwt \
  --http 0.0.0.0:7432
```

Start the auth/rest broker:

Note: to enable the rest broker you need to replace the stub subzero-core crate with the real one.

```sh
cargo add -p proxy subzero-core --git https://github.com/neondatabase/subzero --rev 396264617e78e8be428682f87469bb25429af88a
```

```sh
LOGFMT=text OTEL_SDK_DISABLED=true cargo run --bin proxy --features testing,rest_broker -- \
  -c server.crt -k server.key \
  --is-auth-broker true \
  --is-rest-broker true \
  --wss 0.0.0.0:8080 \
  --http 0.0.0.0:7002 \
  --auth-backend local
```

Create a JWT in your auth provider (e.g. Clerk) and set it in the `NEON_JWT` environment variable.
```sh
export NEON_JWT="..."
```

Run a query against the auth broker:
```sh
curl -k "https://foo.local.neon.build:8080/sql" \
  -H "Authorization: Bearer $NEON_JWT" \
  -H "neon-connection-string: postgresql://authenticator@foo.local.neon.build/database" \
  -d '{"query":"select 1","params":[]}'
```

Make a rest request against the auth broker (rest broker):
```sh
curl -k "https://foo.local.neon.build:8080/database/rest/v1/items?select=id,name&id=eq.1" \
-H "Authorization: Bearer $NEON_JWT"
```
