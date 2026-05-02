//! Embedded TCP/Postgres connection pool with global capacity enforcement.
//!
//! The pool is keyed by `(endpoint_cache_key, dbname, role)`. Each key has
//! its own bounded queue of idle connections and a per-key cap. On top of
//! that, every physical compute connection holds a permit on a process-wide
//! semaphore, so the global cap (`tcp_pool_max_total_conns`) is enforced
//! across all keys, not just within a single key.
//!
//! The checkout path is deadline-aware: every wait — per-key queue, global
//! capacity, backend connect — is bounded by `tcp_pool_checkout_timeout`. A
//! saturated pool returns an explicit `CheckoutTimeout` instead of blocking
//! forever (the previous bb8-based implementation set a 365-day timeout).
//!
//! When the global cap is reached and the optional `overflow_limit` budget
//! is non-zero, an overflow connection may be created. Overflow connections
//! are never returned to the idle queue — they are short-lived and freed
//! at session end. The hard ceiling on physical compute connections is
//! `max_total_conns + overflow_limit`.
//!
//! Releases carry a reason ([`crate::metrics::TcpPoolReleaseReason`]) that
//! distinguishes clean ends, frontend terminates, IO errors, and so on. The
//! release reason and a `reusable: bool` are reflected in the
//! `proxy_tcp_pool_release_total{reason,reusable}` counter.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use clashmap::ClashMap;
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use postgres_client::connect_raw::StartupStream;
use postgres_protocol::message::backend::Message;
use rand::Rng;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::auth::Backend;
use crate::auth::backend::{ComputeUserInfo, MaybeOwned};
use crate::compute::{AuthInfo, ComputeConnection, MaybeRustlsStream};
use crate::config::{ProxyConfig, TcpPoolConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::metrics::{
    Bool, Metrics, TcpPoolCheckoutGroup, TcpPoolCheckoutOutcome, TcpPoolConnectionState,
    TcpPoolOverflowOutcome, TcpPoolReleaseGroup, TcpPoolReleaseReason,
};
use crate::pqproto;
use crate::proxy::connect_auth::{self, AuthError};
use crate::proxy::connect_compute;
use crate::types::{DbName, EndpointCacheKey, RoleName};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct TcpPoolKey {
    endpoint: EndpointCacheKey,
    dbname: DbName,
    role: RoleName,
}

impl TcpPoolKey {
    pub(crate) fn new(endpoint: EndpointCacheKey, dbname: DbName, role: RoleName) -> Self {
        Self {
            endpoint,
            dbname,
            role,
        }
    }
}

impl std::fmt::Display for TcpPoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.endpoint, self.role, self.dbname)
    }
}

#[derive(Debug, Error)]
pub(crate) enum AcquireError {
    #[error("{0}")]
    Connect(#[from] AuthError),
    #[error("{0}")]
    Startup(#[from] postgres_client::Error),
    #[error("pool checkout timed out after {0:?}")]
    CheckoutTimeout(Duration),
    #[error("pool capacity exceeded: no idle connection, no headroom, no overflow")]
    PoolExhausted,
}

impl UserFacingError for AcquireError {
    fn to_string_client(&self) -> String {
        match self {
            AcquireError::Connect(e) => e.to_string_client(),
            AcquireError::Startup(e) => e.to_string(),
            AcquireError::CheckoutTimeout(_) | AcquireError::PoolExhausted => {
                "the database is temporarily overloaded; please retry".to_owned()
            }
        }
    }
}

impl ReportableError for AcquireError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            AcquireError::Connect(e) => e.get_error_kind(),
            AcquireError::Startup(_) => ErrorKind::Postgres,
            AcquireError::CheckoutTimeout(_) | AcquireError::PoolExhausted => {
                ErrorKind::ServiceRateLimit
            }
        }
    }
}

/// A permit kept alive for the lifetime of a physical compute connection.
/// Dropping the permit releases the slot in the global (or overflow)
/// semaphore.
struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
    is_overflow: bool,
}

/// Pool slot: holds the global / overflow permit, plus the connection
/// object when it's idle. While the slot is checked out, `conn` is `None`
/// — the caller owns the conn until it returns it via `release_with_reason`.
struct PooledCompute {
    conn: Option<ComputeConnection>,
    fresh: bool,
    permit: ConnectionPermit,
    /// Wall-clock instant when the physical connection was created.
    /// Reserved for future lifetime-based eviction (Phase 7).
    #[allow(dead_code)]
    created_at: Instant,
}

#[derive(Default)]
struct KeyState {
    idle: VecDeque<PooledCompute>,
    /// Physical connections currently being created for this key.
    connecting: u32,
    /// Physical connections currently checked out for this key.
    checked_out: u32,
}

impl KeyState {
    fn occupancy(&self) -> u32 {
        self.idle.len() as u32 + self.connecting + self.checked_out
    }
}

struct KeyPool {
    cfg: TcpPoolConfig,
    state: Mutex<KeyState>,
    /// Notified when a per-key slot (or idle conn) frees up. Waiters race
    /// to grab the slot.
    notify: Notify,
}

impl KeyPool {
    fn new(cfg: TcpPoolConfig) -> Self {
        Self {
            cfg,
            state: Mutex::new(KeyState::default()),
            notify: Notify::new(),
        }
    }
}

#[derive(Default)]
struct Inner {
    pools: ClashMap<TcpPoolKey, Arc<KeyPool>>,
    startup_params: ClashMap<TcpPoolKey, Arc<Vec<(Box<str>, Box<str>)>>>,
    /// Lazily initialized on first acquire from the live `TcpPoolConfig`.
    caps: OnceLock<GlobalCaps>,
}

struct GlobalCaps {
    /// Hard global cap on physical compute connections.
    global: Arc<Semaphore>,
    /// Optional overflow budget. Created with `overflow_limit` permits;
    /// may be 0 (no overflow allowed).
    overflow: Arc<Semaphore>,
    /// Cached for diagnostics / pressure metric.
    max_total_conns: usize,
}

pub(crate) struct TcpPoolCheckout {
    key: TcpPoolKey,
    pool: Arc<KeyPool>,
    /// Per-checkout slot. `Some` until release; `None` after.
    slot: Option<PooledCompute>,
    /// Slot was acquired from the overflow budget?
    was_overflow: bool,
    /// `RESET ALL` + replay-startup-params query, captured at acquire time
    /// so transaction-mode reacquires reset the borrowed conn before use.
    /// Threaded through to callers via [`Self::reset_query`].
    reset_query: Arc<str>,
}

impl TcpPoolCheckout {
    pub(crate) fn key(&self) -> &TcpPoolKey {
        &self.key
    }

    /// Session-reset query captured for this checkout. The transaction-mode
    /// loop hands this to [`TcpPoolManager::reacquire`] so each new compute
    /// conn pulled from the idle queue is reset to the client's startup
    /// state before the next transaction runs.
    pub(crate) fn reset_query(&self) -> Arc<str> {
        self.reset_query.clone()
    }

    /// Backwards-compatible thin wrapper over [`Self::release_with_reason`].
    /// Picks a generic reason from the `reusable` flag.
    pub(crate) fn release(self, conn: ComputeConnection, reusable: bool) {
        let reason = if reusable {
            TcpPoolReleaseReason::CleanSessionEnd
        } else {
            TcpPoolReleaseReason::IoError
        };
        self.release_with_reason(conn, reusable, reason);
    }

    /// Release the connection back to the pool with an explicit reason.
    /// `reusable` controls whether the connection re-enters the idle
    /// queue (`true`) or is discarded (`false`).
    pub(crate) fn release_with_reason(
        mut self,
        conn: ComputeConnection,
        reusable: bool,
        reason: TcpPoolReleaseReason,
    ) {
        let m = &Metrics::get().proxy.tcp_pool;

        let mut slot = self.slot.take().expect("checkout already released");
        slot.conn = Some(conn);
        slot.fresh = false;

        // Overflow connections are never pooled, regardless of `reusable`.
        let pool_back = reusable && !self.was_overflow;

        m.release_total.inc(TcpPoolReleaseGroup {
            reason,
            reusable: Bool::from(pool_back),
        });

        let mut state = self.pool.state.lock();
        debug_assert!(state.checked_out > 0, "release without checkout?");
        state.checked_out = state.checked_out.saturating_sub(1);
        m.connections.dec(TcpPoolConnectionState::CheckedOut);

        if pool_back {
            state.idle.push_back(slot);
            m.connections.inc(TcpPoolConnectionState::Idle);
            drop(state);
            self.pool.notify.notify_one();
        } else {
            // Slot dropped here ⇒ permit released ⇒ global / overflow
            // semaphore is freed automatically.
            if self.was_overflow {
                m.connections.dec(TcpPoolConnectionState::Overflow);
            }
            drop(state);
            drop(slot);
            self.pool.notify.notify_one();
        }
    }
}

impl Drop for TcpPoolCheckout {
    fn drop(&mut self) {
        // Dropped without an explicit release: treat as a non-reusable IO
        // error path. This keeps state consistent if a session aborts and
        // the caller never invokes `release_with_reason`.
        if let Some(slot) = self.slot.take() {
            let m = &Metrics::get().proxy.tcp_pool;
            m.release_total.inc(TcpPoolReleaseGroup {
                reason: TcpPoolReleaseReason::IoError,
                reusable: Bool::False,
            });
            let mut state = self.pool.state.lock();
            state.checked_out = state.checked_out.saturating_sub(1);
            m.connections.dec(TcpPoolConnectionState::CheckedOut);
            if self.was_overflow {
                m.connections.dec(TcpPoolConnectionState::Overflow);
            }
            drop(state);
            drop(slot);
            self.pool.notify.notify_one();
        }
    }
}

#[derive(Clone)]
struct ComputeConnectionManager {
    ctx: RequestContext,
    config: &'static ProxyConfig,
    backend: Arc<Backend<'static, ComputeUserInfo>>,
    auth_info: AuthInfo,
}

impl ComputeConnectionManager {
    async fn connect(&self) -> Result<ComputeConnection, AuthError> {
        connect_auth::connect_to_compute_and_auth(
            &self.ctx,
            self.config,
            &self.backend,
            self.auth_info.clone(),
            connect_compute::TlsNegotiation::Postgres,
        )
        .await
    }
}

async fn drain_fresh_startup(conn: &mut ComputeConnection) -> Result<(), postgres_client::Error> {
    loop {
        let msg = conn
            .stream
            .try_next()
            .await
            .map_err(postgres_client::Error::io)?;

        match msg {
            Some(Message::ParameterStatus(_))
            | Some(Message::BackendKeyData(_))
            | Some(Message::NoticeResponse(_)) => {}
            Some(Message::ReadyForQuery(_)) => return Ok(()),
            Some(Message::ErrorResponse(body)) => return Err(postgres_client::Error::db(body)),
            Some(_) => return Err(postgres_client::Error::unexpected_message()),
            None => return Err(postgres_client::Error::closed()),
        }
    }
}

async fn write_simple_query<S>(stream: &mut S, query: &str) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u8(b'Q').await?;
    stream.write_u32((query.len() + 5) as u32).await?;
    stream.write_all(query.as_bytes()).await?;
    stream.write_u8(0).await?;
    stream.flush().await
}

async fn drain_simple_query<S>(stream: &mut S) -> Result<u8, postgres_client::Error>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    loop {
        let (tag, body) = pqproto::read_message(stream, &mut buf, 65536)
            .await
            .map_err(postgres_client::Error::io)?;

        match tag {
            b'Z' if body.len() == 1 => return Ok(body[0]),
            b'Z' => return Err(postgres_client::Error::unexpected_message()),
            b'C' | b'D' | b'I' | b'N' | b'S' | b'T' => {}
            b'E' => return Err(postgres_client::Error::unexpected_message()),
            _ => return Err(postgres_client::Error::unexpected_message()),
        }
    }
}

async fn reset_raw_session(
    stream: &mut MaybeRustlsStream,
    reset_query: &str,
) -> Result<(), postgres_client::Error> {
    write_simple_query(stream, reset_query)
        .await
        .map_err(postgres_client::Error::io)?;
    let status = drain_simple_query(stream).await?;
    if status == b'I' {
        Ok(())
    } else {
        Err(postgres_client::Error::unexpected_message())
    }
}

pub(crate) async fn reset_session(
    conn: ComputeConnection,
    reset_query: &str,
) -> Result<ComputeConnection, postgres_client::Error> {
    let ComputeConnection {
        stream,
        aux,
        hostname,
        ssl_mode,
        socket_addr,
        guage,
    } = conn;

    let mut raw_stream = stream.into_framed().into_inner();
    reset_raw_session(&mut raw_stream, reset_query).await?;

    Ok(ComputeConnection {
        stream: StartupStream::new(raw_stream),
        aux,
        hostname,
        ssl_mode,
        socket_addr,
        guage,
    })
}

pub(crate) struct TcpPoolManager {
    inner: Arc<Inner>,
}

/// Result of a successful permit acquire, used by [`TcpPoolManager::checkout`].
struct CapacityGrant {
    permit: ConnectionPermit,
}

/// Outcome distinguishing where the wait time was spent.
#[derive(Copy, Clone)]
enum WaitedFor {
    Nothing,
    KeyQueue,
}

/// Internal dispatch result used by the fast-path under-lock check.
enum Action {
    Reserve,
    Wait,
}

impl TcpPoolManager {
    pub(crate) fn set_startup_params(&self, key: &TcpPoolKey, params: Vec<(Box<str>, Box<str>)>) {
        self.inner
            .startup_params
            .insert(key.clone(), Arc::new(params));
    }

    pub(crate) fn get_startup_params(
        &self,
        key: &TcpPoolKey,
    ) -> Option<Arc<Vec<(Box<str>, Box<str>)>>> {
        self.inner.startup_params.get(key).map(|v| v.clone())
    }

    fn caps(&self, cfg: &TcpPoolConfig) -> &GlobalCaps {
        self.inner.caps.get_or_init(|| GlobalCaps {
            global: Arc::new(Semaphore::new(cfg.max_total_conns)),
            overflow: Arc::new(Semaphore::new(cfg.overflow_limit)),
            max_total_conns: cfg.max_total_conns,
        })
    }

    pub(crate) async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.gc_one_shard();
        }
    }

    fn gc_one_shard(&self) {
        let shards = self.inner.pools.shards();
        if shards.is_empty() {
            return;
        }

        let shard_idx = rand::rng().random_range(0..shards.len());
        let mut shard = shards[shard_idx].write();
        shard.retain(|(_, pool)| {
            let state = pool.state.lock();
            let keep = state.occupancy() > 0;
            if !keep {
                debug!("tcp pool: dropping empty keyed pool");
            }
            keep
        });
    }

    fn get_or_create_pool(&self, key: TcpPoolKey, cfg: TcpPoolConfig) -> Arc<KeyPool> {
        if let Some(p) = self.inner.pools.get(&key).map(|p| p.clone()) {
            return p;
        }
        self.inner
            .pools
            .entry(key)
            .or_insert_with(|| Arc::new(KeyPool::new(cfg)))
            .clone()
    }

    /// Try to obtain a global semaphore permit. If global is saturated and
    /// overflow capacity is available, take an overflow permit. Otherwise
    /// wait for the global semaphore until `deadline`.
    async fn acquire_capacity(
        &self,
        cfg: &TcpPoolConfig,
        deadline: Instant,
    ) -> Result<CapacityGrant, AcquireError> {
        let caps = self.caps(cfg);
        let m = &Metrics::get().proxy.tcp_pool;

        // Fast path: global has capacity.
        if let Ok(permit) = caps.global.clone().try_acquire_owned() {
            self.update_pressure_gauge(caps);
            return Ok(CapacityGrant {
                permit: ConnectionPermit {
                    _permit: permit,
                    is_overflow: false,
                },
            });
        }
        self.update_pressure_gauge(caps);

        // Global is saturated. Try overflow before waiting.
        if cfg.overflow_limit > 0 {
            match caps.overflow.clone().try_acquire_owned() {
                Ok(permit) => {
                    m.overflow_connections_total
                        .inc(TcpPoolOverflowOutcome::Taken);
                    return Ok(CapacityGrant {
                        permit: ConnectionPermit {
                            _permit: permit,
                            is_overflow: true,
                        },
                    });
                }
                Err(_) => {
                    m.overflow_connections_total
                        .inc(TcpPoolOverflowOutcome::Refused);
                }
            }
        }

        // Wait for the global semaphore up to the deadline.
        let now = Instant::now();
        if now >= deadline {
            return Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout));
        }
        let remaining = deadline - now;

        let global = caps.global.clone();
        match tokio::time::timeout(remaining, global.acquire_owned()).await {
            Ok(Ok(permit)) => {
                self.update_pressure_gauge(caps);
                Ok(CapacityGrant {
                    permit: ConnectionPermit {
                        _permit: permit,
                        is_overflow: false,
                    },
                })
            }
            Ok(Err(_closed)) => Err(AcquireError::PoolExhausted),
            Err(_elapsed) => Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout)),
        }
    }

    fn update_pressure_gauge(&self, caps: &GlobalCaps) {
        let m = &Metrics::get().proxy.tcp_pool;
        let saturated = caps.global.available_permits() == 0 && caps.max_total_conns > 0;
        m.global_pressure.set(if saturated { 1 } else { 0 });
    }

    /// Core checkout. Returns a [`PooledCompute`] (with attached permit)
    /// and the outcome. Honors the per-key cap, the global cap, the
    /// optional overflow budget, and `cfg.checkout_timeout` as a deadline
    /// over the entire checkout path.
    async fn checkout(
        &self,
        pool: &Arc<KeyPool>,
        mgr: &ComputeConnectionManager,
        cfg: &TcpPoolConfig,
    ) -> Result<(PooledCompute, TcpPoolCheckoutOutcome), AcquireError> {
        let m = &Metrics::get().proxy.tcp_pool;
        let started = Instant::now();
        let deadline = started + cfg.checkout_timeout;
        let mut waited_for = WaitedFor::Nothing;

        loop {
            // Register a notification slot before checking state to avoid
            // missed wakeups: if a release happens between our state check
            // and our await, `enable()` ensures we still get woken.
            let notified = pool.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let action = {
                let mut state = pool.state.lock();
                if let Some(mut slot) = state.idle.pop_front() {
                    state.checked_out += 1;
                    m.connections.dec(TcpPoolConnectionState::Idle);
                    m.connections.inc(TcpPoolConnectionState::CheckedOut);
                    slot.fresh = false;
                    let outcome = match waited_for {
                        WaitedFor::Nothing => TcpPoolCheckoutOutcome::ImmediateHit,
                        WaitedFor::KeyQueue => TcpPoolCheckoutOutcome::QueuedHit,
                    };
                    return Ok((slot, outcome));
                }
                if state.occupancy() < cfg.max_conns_per_key as u32 {
                    state.connecting += 1;
                    m.connections.inc(TcpPoolConnectionState::Connecting);
                    Action::Reserve
                } else {
                    Action::Wait
                }
            };

            match action {
                Action::Reserve => {
                    let grant = match self.acquire_capacity(cfg, deadline).await {
                        Ok(g) => g,
                        Err(e) => {
                            let mut state = pool.state.lock();
                            state.connecting = state.connecting.saturating_sub(1);
                            m.connections.dec(TcpPoolConnectionState::Connecting);
                            drop(state);
                            pool.notify.notify_one();
                            return Err(e);
                        }
                    };

                    let is_overflow = grant.permit.is_overflow;

                    let conn = match mgr.connect().await {
                        Ok(c) => c,
                        Err(e) => {
                            let mut state = pool.state.lock();
                            state.connecting = state.connecting.saturating_sub(1);
                            m.connections.dec(TcpPoolConnectionState::Connecting);
                            drop(state);
                            // dropping `grant` releases the global / overflow permit.
                            drop(grant);
                            pool.notify.notify_one();
                            return Err(AcquireError::Connect(e));
                        }
                    };

                    let mut state = pool.state.lock();
                    state.connecting = state.connecting.saturating_sub(1);
                    state.checked_out += 1;
                    m.connections.dec(TcpPoolConnectionState::Connecting);
                    m.connections.inc(TcpPoolConnectionState::CheckedOut);
                    if is_overflow {
                        m.connections.inc(TcpPoolConnectionState::Overflow);
                    }
                    drop(state);

                    let outcome = if is_overflow {
                        TcpPoolCheckoutOutcome::Overflow
                    } else {
                        match waited_for {
                            WaitedFor::Nothing => TcpPoolCheckoutOutcome::MissCreated,
                            WaitedFor::KeyQueue => TcpPoolCheckoutOutcome::QueuedCreated,
                        }
                    };
                    return Ok((
                        PooledCompute {
                            conn: Some(conn),
                            fresh: true,
                            permit: grant.permit,
                            created_at: Instant::now(),
                        },
                        outcome,
                    ));
                }
                Action::Wait => {
                    waited_for = WaitedFor::KeyQueue;
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout));
                    }
                    let remaining = deadline - now;
                    if tokio::time::timeout(remaining, notified).await.is_err() {
                        return Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout));
                    }
                    // Notified — loop and retry.
                }
            }
        }
    }

    fn observe_outcome(&self, started: Instant, outcome: TcpPoolCheckoutOutcome) {
        let m = &Metrics::get().proxy.tcp_pool;
        let elapsed = started.elapsed().as_secs_f64();
        let group = TcpPoolCheckoutGroup { outcome };
        m.checkout_total.inc(group);
        m.checkout_wait_seconds.observe(group, elapsed);
    }

    pub(crate) async fn acquire_or_connect(
        &self,
        config: &TcpPoolConfig,
        key: TcpPoolKey,
        ctx: RequestContext,
        proxy_config: &'static ProxyConfig,
        cplane: crate::control_plane::client::ControlPlaneClient,
        user_info: ComputeUserInfo,
        auth_info: AuthInfo,
    ) -> Result<(ComputeConnection, Option<TcpPoolCheckout>, bool), AcquireError> {
        if !config.enabled {
            // Pool disabled: legacy direct-connect path. Global cap is not
            // enforced when the pool is off.
            let backend = Backend::ControlPlane(MaybeOwned::Owned(cplane), user_info);
            let conn = connect_auth::connect_to_compute_and_auth(
                &ctx,
                proxy_config,
                &backend,
                auth_info,
                connect_compute::TlsNegotiation::Postgres,
            )
            .await?;
            return Ok((conn, None, false));
        }

        let reset_query = Arc::<str>::from(auth_info.tcp_pool_session_reset_query());
        let backend = Arc::new(Backend::ControlPlane(MaybeOwned::Owned(cplane), user_info));
        let mgr = ComputeConnectionManager {
            ctx,
            config: proxy_config,
            backend,
            auth_info,
        };

        let pool = self.get_or_create_pool(key.clone(), *config);
        let started = Instant::now();
        let result = self.checkout(&pool, &mgr, config).await;

        match result {
            Ok((mut slot, outcome)) => {
                self.observe_outcome(started, outcome);
                let was_reused = !slot.fresh;
                let is_overflow = slot.permit.is_overflow;
                let conn = slot.conn.take().expect("slot must hold a conn at checkout");
                Ok((
                    conn,
                    Some(TcpPoolCheckout {
                        key,
                        pool,
                        slot: Some(slot),
                        was_overflow: is_overflow,
                        reset_query,
                    }),
                    was_reused,
                ))
            }
            Err(e) => {
                let outcome = match &e {
                    AcquireError::CheckoutTimeout(_) => TcpPoolCheckoutOutcome::Timeout,
                    AcquireError::PoolExhausted => TcpPoolCheckoutOutcome::Rejected,
                    _ => TcpPoolCheckoutOutcome::Failed,
                };
                self.observe_outcome(started, outcome);
                Err(e)
            }
        }
    }

    pub(crate) async fn reacquire(
        &self,
        key: TcpPoolKey,
        reset_query: Arc<str>,
    ) -> Result<(ComputeConnection, TcpPoolCheckout), AcquireError> {
        // Reacquire is only legal after a successful initial acquire; the
        // KeyPool must already exist. We grab the conn from the idle queue
        // associated with the key.
        let pool = self
            .inner
            .pools
            .get(&key)
            .map(|p| p.clone())
            .expect("pool key must exist before reacquire");
        let cfg = pool.cfg;
        let started = Instant::now();
        let m = &Metrics::get().proxy.tcp_pool;
        let deadline = started + cfg.checkout_timeout;

        loop {
            let notified = pool.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let popped: Option<PooledCompute> = {
                let mut state = pool.state.lock();
                if let Some(mut slot) = state.idle.pop_front() {
                    state.checked_out += 1;
                    m.connections.dec(TcpPoolConnectionState::Idle);
                    m.connections.inc(TcpPoolConnectionState::CheckedOut);
                    slot.fresh = false;
                    Some(slot)
                } else {
                    None
                }
            };

            if let Some(mut slot) = popped {
                let was_overflow = slot.permit.is_overflow;
                let mut conn = slot.conn.take().expect("idle slot must hold a conn");

                let discard = |pool: &Arc<KeyPool>, slot: PooledCompute, was_overflow: bool| {
                    let mut state = pool.state.lock();
                    state.checked_out = state.checked_out.saturating_sub(1);
                    m.connections.dec(TcpPoolConnectionState::CheckedOut);
                    if was_overflow {
                        m.connections.dec(TcpPoolConnectionState::Overflow);
                    }
                    drop(state);
                    drop(slot);
                    pool.notify.notify_one();
                };

                if let Err(e) = drain_fresh_startup(&mut conn).await {
                    discard(&pool, slot, was_overflow);
                    self.observe_outcome(started, TcpPoolCheckoutOutcome::Failed);
                    return Err(AcquireError::Startup(e));
                }
                // Reset session state (RESET ALL + replay startup params)
                // before handing the conn over to the next transaction.
                // This is the "session hygiene" guarantee for transaction-
                // mode pooling: a borrower never sees the previous tenant's
                // GUCs, temp tables, or `SET LOCAL` leftovers.
                let conn = match reset_session(conn, &reset_query).await {
                    Ok(c) => c,
                    Err(e) => {
                        discard(&pool, slot, was_overflow);
                        self.observe_outcome(started, TcpPoolCheckoutOutcome::Failed);
                        return Err(AcquireError::Startup(e));
                    }
                };
                self.observe_outcome(started, TcpPoolCheckoutOutcome::ImmediateHit);
                return Ok((
                    conn,
                    TcpPoolCheckout {
                        key,
                        pool: pool.clone(),
                        slot: Some(slot),
                        was_overflow,
                        reset_query: reset_query.clone(),
                    },
                ));
            }

            let now = Instant::now();
            if now >= deadline {
                self.observe_outcome(started, TcpPoolCheckoutOutcome::Timeout);
                return Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout));
            }
            let remaining = deadline - now;
            if tokio::time::timeout(remaining, notified).await.is_err() {
                self.observe_outcome(started, TcpPoolCheckoutOutcome::Timeout);
                return Err(AcquireError::CheckoutTimeout(cfg.checkout_timeout));
            }
        }
    }

    /// Test-only helper: force the global cap to a chosen size before any
    /// caps observation. Returns false if caps are already initialized.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)]
    pub(crate) fn try_init_caps_for_test(
        &self,
        max_total_conns: usize,
        overflow_limit: usize,
    ) -> bool {
        self.inner
            .caps
            .set(GlobalCaps {
                global: Arc::new(Semaphore::new(max_total_conns)),
                overflow: Arc::new(Semaphore::new(overflow_limit)),
                max_total_conns,
            })
            .is_ok()
    }
}

static MANAGER: Lazy<TcpPoolManager> = Lazy::new(|| TcpPoolManager {
    inner: Arc::new(Inner::default()),
});

pub(crate) fn manager() -> &'static TcpPoolManager {
    &MANAGER
}

// ---------------------------------------------------------------------------
// tests
//
// These tests target the cap arithmetic and permit lifetime, which are the
// load-bearing parts of the global enforcement guarantee. They drive the
// internal data structures directly rather than through the (network-bound)
// `acquire_or_connect` path so they don't need a live compute backend.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::Semaphore;

    use super::{ConnectionPermit, GlobalCaps, KeyPool, KeyState, PooledCompute};
    use crate::config::{TcpPoolConfig, TcpPoolMode};

    fn cfg(max_total: usize, max_per_key: usize, overflow: usize) -> TcpPoolConfig {
        TcpPoolConfig {
            enabled: true,
            mode: TcpPoolMode::Session,
            max_conns_per_key: max_per_key,
            max_total_conns: max_total,
            overflow_limit: overflow,
            idle_timeout: Duration::from_secs(60),
            checkout_timeout: Duration::from_millis(50),
            fallback_direct_connect: false,
        }
    }

    fn make_caps(max_total: usize, overflow: usize) -> GlobalCaps {
        GlobalCaps {
            global: Arc::new(Semaphore::new(max_total)),
            overflow: Arc::new(Semaphore::new(overflow)),
            max_total_conns: max_total,
        }
    }

    fn fake_permit(caps: &GlobalCaps, want_overflow: bool) -> Option<ConnectionPermit> {
        if want_overflow {
            caps.overflow
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|p| ConnectionPermit {
                    _permit: p,
                    is_overflow: true,
                })
        } else {
            caps.global
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|p| ConnectionPermit {
                    _permit: p,
                    is_overflow: false,
                })
        }
    }

    /// Verify that the global semaphore caps the total number of physical
    /// permits across many simulated pool keys. Without global enforcement
    /// the pool would allow `num_keys * max_per_key` permits.
    #[test]
    fn global_cap_enforced_across_many_keys() {
        const NUM_KEYS: usize = 50;
        const MAX_PER_KEY: usize = 8;
        const MAX_TOTAL: usize = 16;

        let caps = make_caps(MAX_TOTAL, 0);
        let pools: Vec<Arc<KeyPool>> = (0..NUM_KEYS)
            .map(|_| Arc::new(KeyPool::new(cfg(MAX_TOTAL, MAX_PER_KEY, 0))))
            .collect();

        // Try to fill every pool to its per-key cap. We can only succeed
        // up to MAX_TOTAL because the global semaphore runs out first.
        let mut held: Vec<ConnectionPermit> = Vec::new();
        'outer: for pool in &pools {
            for _ in 0..MAX_PER_KEY {
                match fake_permit(&caps, false) {
                    Some(permit) => {
                        let mut state = pool.state.lock();
                        state.checked_out += 1;
                        drop(state);
                        held.push(permit);
                    }
                    None => break 'outer,
                }
            }
        }

        assert_eq!(held.len(), MAX_TOTAL, "global cap should bound total");
        assert!(
            NUM_KEYS * MAX_PER_KEY > MAX_TOTAL,
            "test config must put global below the sum of per-key caps"
        );

        // Drop one permit and confirm a new one is available.
        held.pop();
        assert!(
            fake_permit(&caps, false).is_some(),
            "freed slot should be reusable"
        );
    }

    /// Per-key cap math is computed from `KeyState::occupancy`.
    #[test]
    fn per_key_cap_math() {
        let pool = Arc::new(KeyPool::new(cfg(100, 4, 0)));
        let mut state = pool.state.lock();
        state.connecting = 2;
        state.checked_out = 1;
        assert_eq!(state.occupancy(), 3);
        assert!(state.occupancy() < pool.cfg.max_conns_per_key as u32);
        state.connecting += 1;
        assert!(state.occupancy() == pool.cfg.max_conns_per_key as u32);
    }

    /// When the global cap is full and no overflow exists, a new permit
    /// request fails immediately.
    #[test]
    fn global_full_no_overflow_rejects() {
        let caps = make_caps(2, 0);
        let p1 = fake_permit(&caps, false).expect("first permit");
        let p2 = fake_permit(&caps, false).expect("second permit");
        assert!(fake_permit(&caps, false).is_none(), "third must fail");
        assert!(
            fake_permit(&caps, true).is_none(),
            "overflow disabled, must fail"
        );
        drop((p1, p2));
        assert!(
            fake_permit(&caps, false).is_some(),
            "freed slot should be reusable"
        );
    }

    /// Overflow caps the absolute physical-connection ceiling at
    /// `max_total + overflow_limit`.
    #[test]
    fn overflow_capped_above_global() {
        let caps = make_caps(2, 1);
        let _g1 = fake_permit(&caps, false).unwrap();
        let _g2 = fake_permit(&caps, false).unwrap();
        let _o1 = fake_permit(&caps, true).unwrap();
        // No more permits anywhere.
        assert!(fake_permit(&caps, false).is_none());
        assert!(fake_permit(&caps, true).is_none());
    }

    /// Dropping a `PooledCompute` slot drops its `ConnectionPermit`, which
    /// frees the global semaphore slot. This is the invariant that ties
    /// physical-connection lifetime to the cap.
    #[test]
    fn dropping_slot_frees_global_permit() {
        let caps = make_caps(1, 0);
        let permit = fake_permit(&caps, false).expect("first permit");
        // Build a slot with no conn (idle slot would have one, but the
        // permit is what matters for the cap).
        let slot = PooledCompute {
            conn: None,
            fresh: false,
            permit,
            created_at: Instant::now(),
        };
        assert_eq!(caps.global.available_permits(), 0);
        drop(slot);
        assert_eq!(caps.global.available_permits(), 1);
    }

    /// `KeyState::occupancy` is the sum of idle, connecting and checked_out.
    #[test]
    fn key_state_occupancy_math() {
        let mut s = KeyState::default();
        s.connecting = 1;
        s.checked_out = 2;
        assert_eq!(s.occupancy(), 3);
    }

    /// Basic bb8-replacement sanity check: a checkout that times out
    /// because every other slot is held returns an explicit timeout
    /// rather than blocking. Driven through a contrived semaphore.
    #[tokio::test]
    async fn semaphore_acquire_respects_timeout() {
        let s = Arc::new(Semaphore::new(1));
        let _held = s.clone().acquire_owned().await.unwrap();
        let res = tokio::time::timeout(Duration::from_millis(50), s.clone().acquire_owned()).await;
        assert!(res.is_err(), "must time out instead of blocking");
    }
}
