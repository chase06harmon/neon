# Local Neon Proxy Pooling Setup

## 1. Create Postgres compute instances in Docker

```bash
sudo docker run -d --name compute-node-1 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgres:16
```

Then update the `postgres` role password to the SCRAM value expected by the mock control plane:

```bash
docker exec -i compute-node-1 psql -U postgres -d postgres -c 'ALTER ROLE "postgres" WITH LOGIN ENCRYPTED PASSWORD '\''SCRAM-SHA-256$4096:M2ZX/kfDSd3vv5iFO/QNUA==$mookt3EiEpd/vMqGbd7df3qVwfyUfM91Ps72sNewNg4=:3nMi8eBSHggIBNSgAik6lQnE3hQcsS+myylZlYgNA1U='\'';'
```

> **Note:** This SCRAM value is stored verbatim in the mock control plane.

> **Note:** You can create multiple compute nodes by repeating the process above with different container names and ports.

## 2. Create an auth database

Use this only if running with local auth instead of the control plane.

```bash
sudo docker run -d --name auth_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=postgres \
  -p 5434:5432 \
  postgres:16
```

## 3. Generate SSL certificates

```bash
openssl req -new -x509 -days 365 -nodes -text \
  -out server.crt \
  -keyout server.key \
  -subj "/CN=*.local.neon.build"
```

## 4. Create endpoint metadata

From the project root directory, create:

```text
.neon/endpoints/<endpoint_id>/endpoint.json
```

The endpoint format is described at the bottom of this file.

> **Note:** Only `pg_port` is read by the current mock control plane.

> **Note:** This example uses `comp1`, `comp2`, etc. as `<endpoint_id>` values for compute nodes.

## 5. Start the mock control plane REST API

```bash
RUST_LOG=info ./target/debug/proxy-cplane-api \
  --listen 127.0.0.1:3010 \
  --neon-local-repo-dir "$PWD/.neon" \
  --neon-local-bin "$PWD/target/debug/neon_local"
```

## 6. Start the proxy

```bash
NEON_INTERNAL_CA_FILE=/tmp/compute-ca.crt \
RUST_LOG=proxy \
LOGFMT=text \
cargo run -p proxy --bin proxy --features testing -- \
  --auth-backend cplane-v1 \
  --auth-endpoint http://127.0.0.1:3010 \
  --tcp-pool-enabled true \
  --tcp-pool-mode transaction \
  --tcp-pool-max-conns-per-key 2 \
  --tcp-pool-idle-timeout 30s \
  --redis-auth-type plain \
  --redis-plain 'redis://127.0.0.1:6379' \
  -c server.crt \
  -k server.key
```

> **Note:** `--tcp-pool-max-conns-per-key` can be increased above `2`.

## 7. Connect with `psql`

```bash
PGSSLROOTCERT=./server.crt psql "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full"
```

# Correctness Test

## 1. Initialize pgbench tables

```bash
PGSSLROOTCERT=./server.crt pgbench -i -s 10 \
  "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full"
```

## 2. Run pgbench workload

Run pgbench with 20 clients, 4 worker threads, a 60-second duration, and progress output every 5 seconds.

```bash
PGSSLROOTCERT=./server.crt pgbench -c 20 -j 4 -T 60 -P 5 -M simple \
  "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full"
```

You can also run the same workload with `extended` or `prepared` protocol modes:

```bash
PGSSLROOTCERT=./server.crt pgbench -c 20 -j 4 -T 60 -P 5 -M extended \
  "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full"
```

```bash
PGSSLROOTCERT=./server.crt pgbench -c 20 -j 4 -T 60 -P 5 -M prepared \
  "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full"
```

## 3. Run invariant correctness check

After the workload finishes, verify the pgbench accounting invariant:

```bash
PGSSLROOTCERT=./server.crt psql \
  "postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full" \
  -c "WITH sums AS (
        SELECT
          (SELECT COALESCE(sum(abalance), 0) FROM pgbench_accounts) AS accounts_sum,
          (SELECT COALESCE(sum(tbalance), 0) FROM pgbench_tellers) AS tellers_sum,
          (SELECT COALESCE(sum(bbalance), 0) FROM pgbench_branches) AS branches_sum,
          (SELECT COALESCE(sum(delta), 0) FROM pgbench_history) AS history_sum
      )
      SELECT *,
        accounts_sum = tellers_sum
        AND tellers_sum = branches_sum
        AND branches_sum = history_sum AS ok
      FROM sums;"
```

The result should show:

```text
ok = t
```

# Benchmarking

## 1. Start processes

Start the Neon proxy and compute nodes as described above.

Then start PgCat:

```text
https://github.com/postgresml/pgcat
```

## 2. Run benchmarking script

Run the benchmark matrix against `proxy_pgcat`, `proxy_pool`, or `passthrough`:

```bash
env \
  TARGETS='proxy_pgcat proxy_pool' \
  WORKLOADS='readonly_steady readonly_connect tpcb_steady' \
  STEADY_CONCURRENCIES='10 50 100 250' \
  CONNECT_CONCURRENCIES='1 10 25 50 100' \
  STEADY_DURATION='60' \
  CONNECT_DURATION='30' \
  REPS='3' \
  IDLE_WAITS='15,30,45,60,90' \
  WAIT_TARGET_SELECT='1' \
  RESET_WAIT_SECONDS='5' \
  SAMPLE_INTERVAL='1.0' \
  bash scripts/run_tcp_pool_benchmark_matrix.sh
```

[![Neon](https://github.com/user-attachments/assets/fd91da5f-44a9-41c7-9075-36a5b5608083)](https://neon.com)


# Neon

Neon is an open-source serverless Postgres database platform. It separates storage and compute and substitutes the PostgreSQL storage layer by redistributing data across a cluster of nodes.

## Quick start
Try the [Neon Free Tier](https://neon.com/signup) to create a serverless Postgres instance. Then connect to it with your preferred Postgres client (psql, dbeaver, etc) or use the online [SQL Editor](https://neon.com/docs/get-started-with-neon/query-with-neon-sql-editor/). See [Connect from any application](https://neon.com/docs/connect/connect-from-any-app/) for connection instructions.

Alternatively, compile and run the project [locally](#running-local-installation).

## Architecture overview

A Neon installation consists of compute nodes and the Neon storage engine. Compute nodes are stateless PostgreSQL nodes backed by the Neon storage engine.

The Neon storage engine consists of two major components:
- Pageserver: Scalable storage backend for the compute nodes.
- Safekeepers: The safekeepers form a redundant WAL service that received WAL from the compute node, and stores it durably until it has been processed by the pageserver and uploaded to cloud storage.

See developer documentation in [SUMMARY.md](/docs/SUMMARY.md) for more information.

## Running a local development environment

Neon can be run on a workstation for small experiments and to test code changes, by
following these instructions.

#### Installing dependencies on Linux
1. Install build dependencies and other applicable packages

* On Ubuntu or Debian, this set of packages should be sufficient to build the code:
```bash
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev cmake postgresql-client protobuf-compiler \
libprotobuf-dev libcurl4-openssl-dev openssl python3-poetry lsof libicu-dev
```
* On Fedora, these packages are needed:
```bash
dnf install flex bison readline-devel zlib-devel openssl-devel \
  libseccomp-devel perl clang cmake postgresql postgresql-contrib protobuf-compiler \
  protobuf-devel libcurl-devel openssl poetry lsof libicu-devel libpq-devel python3-devel \
  libffi-devel
```
* On Arch based systems, these packages are needed:
```bash
pacman -S base-devel readline zlib libseccomp openssl clang \
postgresql-libs cmake postgresql protobuf curl lsof
```

Building Neon requires 3.15+ version of `protoc` (protobuf-compiler). If your distribution provides an older version, you can install a newer version from [here](https://github.com/protocolbuffers/protobuf/releases).

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Installing dependencies on macOS (12.3.1)
1. Install XCode and dependencies
```
xcode-select --install
brew install protobuf openssl flex bison icu4c pkg-config m4

# add openssl to PATH, required for ed25519 keys generation in neon_local
echo 'export PATH="$(brew --prefix openssl)/bin:$PATH"' >> ~/.zshrc
```

If you get errors about missing `m4` you may have to install it manually:
```
brew install m4
brew link --force m4
```

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

3. Install PostgreSQL Client
```
# from https://stackoverflow.com/questions/44654216/correct-way-to-install-psql-without-full-postgres-on-macos
brew install libpq
brew link --force libpq
```

#### Rustc version

The project uses [rust toolchain file](./rust-toolchain.toml) to define the version it's built with in CI for testing and local builds.

This file is automatically picked up by [`rustup`](https://rust-lang.github.io/rustup/overrides.html#the-toolchain-file) that installs (if absent) and uses the toolchain version pinned in the file.

rustup users who want to build with another toolchain can use the [`rustup override`](https://rust-lang.github.io/rustup/overrides.html#directory-overrides) command to set a specific toolchain for the project's directory.

non-rustup users most probably are not getting the same toolchain automatically from the file, so are responsible to manually verify that their toolchain matches the version in the file.
Newer rustc versions most probably will work fine, yet older ones might not be supported due to some new features used by the project or the crates.

#### Building on Linux

1. Build neon and patched postgres
```
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`nproc` -s"
# Remove -s for the verbose build log

make -j`nproc` -s
```

#### Building on OSX

1. Build neon and patched postgres
```
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`sysctl -n hw.logicalcpu` -s"
# Remove -s for the verbose build log

make -j`sysctl -n hw.logicalcpu` -s
```

#### Dependency installation notes
To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `pg_install/bin` and `pg_install/lib`, respectively.

To run the integration tests or Python scripts (not required to use the code), install
Python (3.11 or higher), and install the python3 packages using `./scripts/pysync` (requires [poetry>=1.8](https://python-poetry.org/)) in the project directory.


#### Running neon database
1. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .neon with proper paths to binaries and data
# Later that would be responsibility of a package install script
> cargo neon init
Initializing pageserver node 1 at '127.0.0.1:64000' in ".neon"

# start pageserver, safekeeper, and broker for their intercommunication
> cargo neon start
Starting neon broker at 127.0.0.1:50051.
storage_broker started, pid: 2918372
Starting pageserver node 1 at '127.0.0.1:64000' in ".neon".
pageserver started, pid: 2918386
Starting safekeeper at '127.0.0.1:5454' in '.neon/safekeepers/sk1'.
safekeeper 1 started, pid: 2918437

# create initial tenant and use it as a default for every future neon_local invocation
> cargo neon tenant create --set-default
tenant 9ef87a5bf0d92544f6fafeeb3239695c successfully created on the pageserver
Created an initial timeline 'de200bd42b49cc1814412c7e592dd6e9' at Lsn 0/16B5A50 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c
Setting tenant 9ef87a5bf0d92544f6fafeeb3239695c as a default one

# create postgres compute node
> cargo neon endpoint create main

# start postgres compute node
> cargo neon endpoint start main
Starting new endpoint main (PostgreSQL v14) on timeline de200bd42b49cc1814412c7e592dd6e9 ...
Starting postgres at 'postgresql://cloud_admin@127.0.0.1:55432/postgres'

# check list of running postgres instances
> cargo neon endpoint list
 ENDPOINT  ADDRESS          TIMELINE                          BRANCH NAME  LSN        STATUS
 main      127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main         0/16B5BA8  running
```

2. Now, it is possible to connect to postgres and run some queries:
```text
> psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1,1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

3. And create branches and run postgres on them:
```sh
# create branch named migration_check
> cargo neon timeline branch --branch-name migration_check
Created timeline 'b3b863fa45fa9e57e615f9f2d944e601' at Lsn 0/16F9A00 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c. Ancestor timeline: 'main'

# check branches tree
> cargo neon timeline list
(L) main [de200bd42b49cc1814412c7e592dd6e9]
(L) ┗━ @0/16F9A00: migration_check [b3b863fa45fa9e57e615f9f2d944e601]

# create postgres on that branch
> cargo neon endpoint create migration_check --branch-name migration_check

# start postgres on that branch
> cargo neon endpoint start migration_check
Starting new endpoint migration_check (PostgreSQL v14) on timeline b3b863fa45fa9e57e615f9f2d944e601 ...
Starting postgres at 'postgresql://cloud_admin@127.0.0.1:55434/postgres'

# check the new list of running postgres instances
> cargo neon endpoint list
 ENDPOINT         ADDRESS          TIMELINE                          BRANCH NAME      LSN        STATUS
 main             127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main             0/16F9A38  running
 migration_check  127.0.0.1:55434  b3b863fa45fa9e57e615f9f2d944e601  migration_check  0/16F9A70  running

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1

# check that the new change doesn't affect the 'main' postgres
> psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

4. If you want to run tests afterwards (see below), you must stop all the running pageserver, safekeeper, and postgres instances
   you have just started. You can terminate them all with one command:
```sh
> cargo neon stop
```

More advanced usages can be found at [Local Development Control Plane (`neon_local`))](./control_plane/README.md).

#### Handling build failures

If you encounter errors during setting up the initial tenant, it's best to stop everything (`cargo neon stop`) and remove the `.neon` directory. Then fix the problems, and start the setup again.

## Running tests

### Rust unit tests

We are using [`cargo-nextest`](https://nexte.st/) to run the tests in Github Workflows.
Some crates do not support running plain `cargo test` anymore, prefer `cargo nextest run` instead.
You can install `cargo-nextest` with `cargo install cargo-nextest`.

### Integration tests

Ensure your dependencies are installed as described [here](https://github.com/neondatabase/neon#dependency-installation-notes).

```sh
git clone --recursive https://github.com/neondatabase/neon.git

CARGO_BUILD_FLAGS="--features=testing" make

./scripts/pytest
```

By default, this runs both debug and release modes, and all supported postgres versions. When
testing locally, it is convenient to run just one set of permutations, like this:

```sh
DEFAULT_PG_VERSION=17 BUILD_TYPE=release ./scripts/pytest
```

## Flamegraphs

You may find yourself in need of flamegraphs for software in this repository.
You can use [`flamegraph-rs`](https://github.com/flamegraph-rs/flamegraph) or the original [`flamegraph.pl`](https://github.com/brendangregg/FlameGraph). Your choice!

>[!IMPORTANT]
> If you're using `lld` or `mold`, you need the `--no-rosegment` linker argument.
> It's a [general thing with Rust / lld / mold](https://crbug.com/919499#c16), not specific to this repository.
> See [this PR for further instructions](https://github.com/neondatabase/neon/pull/6764).

## Cleanup

For cleaning up the source tree from build artifacts, run `make clean` in the source directory.

For removing every artifact from build and configure steps, run `make distclean`, and also consider removing the cargo binaries in the `target` directory, as well as the database in the `.neon` directory. Note that removing the `.neon` directory will remove your database, with all data in it. You have been warned!

## Documentation

[docs](/docs) Contains a top-level overview of all available markdown documentation.

- [sourcetree.md](/docs/sourcetree.md) contains overview of source tree layout.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`

See also README files in some source directories, and `rustdoc` style documentation comments.

Other resources:

- [SELECT 'Hello, World'](https://neon.com/blog/hello-world/): Blog post by Nikita Shamgunov on the high level architecture
- [Architecture decisions in Neon](https://neon.com/blog/architecture-decisions-in-neon/): Blog post by Heikki Linnakangas
- [Neon: Serverless PostgreSQL!](https://www.youtube.com/watch?v=rES0yzeERns): Presentation on storage system by Heikki Linnakangas in the CMU Database Group seminar series

### Postgres-specific terms

Due to Neon's very close relation with PostgreSQL internals, numerous specific terms are used.
The same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Neon glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/14/glossary.html)
- Other PostgreSQL documentation and sources (Neon fork sources can be found [here](https://github.com/neondatabase/postgres))

## Join the development

- Read [CONTRIBUTING.md](/CONTRIBUTING.md) to learn about project code style and practices.
- To get familiar with a source tree layout, use [sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html
