//! Compute target load balancer.
//!
//! Sits between the control-plane lookup ("which compute candidates serve
//! this endpoint?") and the bb8 connection pool ("open a new physical
//! backend for this key"). Given a set of candidate compute targets for
//! an endpoint, the load balancer picks one based on a configurable
//! policy. Currently:
//!
//! - `None` — no LB; pick the first candidate. Behaves identically to
//!   the pre-LB code.
//! - `RoundRobin` — atomic counter modulo n.
//! - `P2C` — power-of-two-choices: sample two random candidates, score
//!   each by the number of currently-open backend connections to that
//!   target, return the lower-scoring one. With one candidate, trivially
//!   returns it.
//!
//! P2C is the policy the load-balancing benchmark exercises. The score
//! is "current open backend connections", which is the information the
//! existing TCP pool surfaces — the LB consumes the same lifecycle that
//! Charles Harmon's pool established (every successful backend connect
//! increments; every drop decrements). Per-target state is in-process
//! only; high-cardinality target labels never reach Prometheus.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use rand::Rng;
use tracing::debug;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum LbPolicy {
    /// No load balancing. The first candidate is always picked. Use for
    /// single-target setups or when the LB should be a no-op.
    None,
    /// Round-robin across the candidate list.
    RoundRobin,
    /// Power-of-two-choices using current open-backend counts as score.
    P2c,
}

impl Default for LbPolicy {
    fn default() -> Self {
        LbPolicy::None
    }
}

#[derive(Default)]
struct TargetStats {
    /// Currently-open backend connections to this target.
    open_conns: AtomicUsize,
}

pub struct ComputeLb {
    policy: LbPolicy,
    /// Per-target stats keyed by `host:port`. Only populated for targets
    /// the LB has actually opened connections to; lookups for unseen
    /// targets default to a zero-stats target (which biases the picker
    /// toward unused candidates — desirable on cold start).
    targets: Mutex<HashMap<String, TargetStats>>,
    rr_counter: AtomicUsize,
}

impl ComputeLb {
    pub fn new(policy: LbPolicy) -> Self {
        Self {
            policy,
            targets: Mutex::new(HashMap::new()),
            rr_counter: AtomicUsize::new(0),
        }
    }

    pub fn policy(&self) -> LbPolicy {
        self.policy
    }

    /// Pick one candidate by index. Empty slice ⇒ panic; that's a caller
    /// programming error (the control plane must hand the LB at least
    /// one candidate).
    pub fn pick_index(&self, candidates: &[String]) -> usize {
        assert!(!candidates.is_empty(), "no candidate compute targets");
        if candidates.len() == 1 {
            return 0;
        }
        match self.policy {
            LbPolicy::None => 0,
            LbPolicy::RoundRobin => {
                let i = self.rr_counter.fetch_add(1, Ordering::Relaxed);
                i % candidates.len()
            }
            LbPolicy::P2c => self.pick_p2c(candidates),
        }
    }

    fn pick_p2c(&self, candidates: &[String]) -> usize {
        // Sample two distinct candidate indices; pick the lower-scoring.
        let n = candidates.len();
        let mut rng = rand::rng();
        let i = rng.random_range(0..n);
        let mut j = rng.random_range(0..n - 1);
        if j >= i {
            j += 1;
        }
        let si = self.score(&candidates[i]);
        let sj = self.score(&candidates[j]);
        debug!(
            "p2c sample: {} score={} vs {} score={}",
            candidates[i], si, candidates[j], sj
        );
        if si <= sj { i } else { j }
    }

    fn score(&self, target: &str) -> usize {
        let g = self.targets.lock();
        g.get(target)
            .map(|s| s.open_conns.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Increment the open-conn count for `target`. Returns a guard that
    /// decrements on drop.
    pub fn track_open(&'static self, target: String) -> ComputeLbGuard {
        let mut g = self.targets.lock();
        let entry = g.entry(target.clone()).or_default();
        entry.open_conns.fetch_add(1, Ordering::Relaxed);
        ComputeLbGuard {
            lb: self,
            target: Some(target),
        }
    }

    /// Snapshot of current per-target open-conn counts. Test/diagnostic
    /// helper; not exported as a metric.
    #[cfg(test)]
    pub fn snapshot(&self) -> Vec<(String, usize)> {
        let g = self.targets.lock();
        g.iter()
            .map(|(k, v)| (k.clone(), v.open_conns.load(Ordering::Relaxed)))
            .collect()
    }
}

/// Decrement the per-target open-conn count when the connection it was
/// associated with is dropped. Optional in `ComputeConnection` so that
/// non-LB paths (e.g. single-target wake) don't pay any overhead.
pub struct ComputeLbGuard {
    lb: &'static ComputeLb,
    target: Option<String>,
}

impl Drop for ComputeLbGuard {
    fn drop(&mut self) {
        let Some(target) = self.target.take() else {
            return;
        };
        let g = self.lb.targets.lock();
        if let Some(stats) = g.get(&target) {
            stats.open_conns.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

static LB: OnceLock<ComputeLb> = OnceLock::new();

/// Initialize the global LB instance. Should be called once at startup
/// from `build_config` after CLI parsing. Subsequent calls are no-ops
/// (returns the existing instance), so ordering is safe.
pub fn init(policy: LbPolicy) -> &'static ComputeLb {
    LB.get_or_init(|| ComputeLb::new(policy))
}

/// Access the global LB instance. Panics if `init` was not called first.
pub fn lb() -> &'static ComputeLb {
    LB.get()
        .expect("compute_lb::init must be called at startup")
}

/// Best-effort access for code paths that may run before `init`. Returns
/// `None` if the LB has not been initialized yet — used by mock
/// control-plane wake which must work in tests where `init` is skipped.
pub fn lb_opt() -> Option<&'static ComputeLb> {
    LB.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single-candidate slice always returns index 0 regardless of
    /// policy.
    #[test]
    fn single_candidate_trivially_picks_zero() {
        for p in [LbPolicy::None, LbPolicy::RoundRobin, LbPolicy::P2c] {
            let lb = ComputeLb::new(p);
            assert_eq!(lb.pick_index(&["only:5432".to_owned()]), 0, "policy={p:?}");
        }
    }

    /// Round-robin distributes evenly over many picks.
    #[test]
    fn round_robin_spreads_evenly() {
        let lb = ComputeLb::new(LbPolicy::RoundRobin);
        let cands = vec!["a:1".to_owned(), "b:2".to_owned(), "c:3".to_owned()];
        let mut counts = [0usize; 3];
        for _ in 0..3000 {
            counts[lb.pick_index(&cands)] += 1;
        }
        for c in counts {
            // 1000 ± a few from atomic ordering jitter on threaded RR; this
            // single-threaded loop should be exact.
            assert_eq!(c, 1000);
        }
    }

    /// P2C with all targets at zero open conns is uniform-ish (sampling).
    #[test]
    fn p2c_uniform_when_unloaded() {
        let lb = ComputeLb::new(LbPolicy::P2c);
        let cands = vec!["a:1".to_owned(), "b:2".to_owned(), "c:3".to_owned()];
        let mut counts = [0usize; 3];
        for _ in 0..3000 {
            counts[lb.pick_index(&cands)] += 1;
        }
        // Each candidate should land near 1000 ± a few hundred. Loose
        // bound because P2C samples randomly when ties; the exact
        // distribution depends on RNG quality.
        for c in counts {
            assert!(c > 700 && c < 1300, "p2c counts skewed: {counts:?}");
        }
    }

    /// P2C avoids the more-loaded target when scores differ.
    #[test]
    fn p2c_avoids_loaded_target() {
        let lb_storage = Box::leak(Box::new(ComputeLb::new(LbPolicy::P2c)));
        let cands = vec!["loaded:1".to_owned(), "free:2".to_owned()];

        // Synthetically load "loaded:1" with 50 open conns.
        let mut guards = Vec::new();
        for _ in 0..50 {
            guards.push(lb_storage.track_open("loaded:1".to_owned()));
        }

        let mut counts = [0usize; 2];
        for _ in 0..3000 {
            counts[lb_storage.pick_index(&cands)] += 1;
        }
        // P2C samples two of two candidates each time → always sees both
        // and picks the lower-loaded one. Should be ~deterministic.
        assert!(
            counts[1] > counts[0] * 5,
            "p2c didn't avoid loaded target: {counts:?}",
        );

        drop(guards);
        assert_eq!(lb_storage.score("loaded:1"), 0);
    }

    /// Dropping a ComputeLbGuard decrements the per-target counter.
    #[test]
    fn guard_drop_decrements() {
        let lb_storage = Box::leak(Box::new(ComputeLb::new(LbPolicy::P2c)));
        let g1 = lb_storage.track_open("t:1".to_owned());
        let g2 = lb_storage.track_open("t:1".to_owned());
        assert_eq!(lb_storage.score("t:1"), 2);
        drop(g1);
        assert_eq!(lb_storage.score("t:1"), 1);
        drop(g2);
        assert_eq!(lb_storage.score("t:1"), 0);
    }
}
