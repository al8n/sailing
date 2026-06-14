use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Rebuild a node from durable storage after a crash. If a durable snapshot exists,
  /// restores the state machine from it first, then replays only the post-snapshot
  /// committed tail `(snapshot.last_index .. commit]`. Without a snapshot, replays the
  /// full committed log from index 1. Returns in `Follower` with the
  /// election timer armed.
  ///
  /// A corrupt durable snapshot poisons the node (no partial state is applied).
  ///
  /// `boot_epoch` MUST be strictly greater than the `boot_epoch` of every prior incarnation of THIS
  /// node, and the caller MUST persist it durably (e.g. a monotonic boot counter) BEFORE calling
  /// `restart`. It namespaces this incarnation's forwarded-read tokens so that a `ReadIndexResp` sent
  /// to a previous incarnation — and redelivered after the restart by a transport that does not drop
  /// pre-crash messages — can never complete a post-restart read at a stale index. A fresh node
  /// ([`Endpoint::new`]) uses epoch 0, so the first `restart` must pass at least 1. Reusing or
  /// decreasing the epoch reopens the stale-read hole; the leader-side read path needs no epoch
  /// because re-acquiring leadership requires a strictly-higher term, which fences stale responses.
  pub fn restart<L, S>(
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Self
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
    I: crate::Data,
  {
    let now: crate::Now = now.into();
    Self::restart_inner(config, now, seed, fsm, boot_epoch, None, log, stable)
  }

  /// Migration entry point: like [`restart`](Self::restart) but for a ONE-TIME upgrade from a binary
  /// that persisted no `lease_support` floor. `assume_prior_lease_support` is an upper bound on
  /// the LeaseBased read-lease window this node may have advertised (in memory) before the crash — typically
  /// the pre-upgrade `election_timeout`. The post-restart vote fence is sized to honor it (so the old
  /// leader's still-live lease cannot be undermined), and it is persisted as the durable floor so every
  /// subsequent plain `restart` is fully covered. Pass `None` (or just use `restart`) once any enforcing
  /// restart has recorded a real floor; a too-small value reopens the config-drift hole for exactly one
  /// restart, and `None` means "trust only the durable record".
  // Mirrors `restart`'s wide recovery API plus the one migration parameter; bundling into a struct would
  // obscure the parallel with `restart`/`new`.
  #[allow(clippy::too_many_arguments)]
  pub fn restart_migrating<L, S>(
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<core::time::Duration>,
    log: &mut L,
    stable: &mut S,
  ) -> Self
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
    I: crate::Data,
  {
    let now: crate::Now = now.into();
    Self::restart_inner(
      config,
      now,
      seed,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      log,
      stable,
    )
  }

  #[allow(clippy::too_many_arguments)]
  pub(crate) fn restart_inner<L, S>(
    config: Config<I>,
    now: crate::Now,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<core::time::Duration>,
    log: &mut L,
    stable: &mut S,
  ) -> Self
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
    I: crate::Data, // decode ConfChangeV2 entries when replaying the log's membership (Raft §4.1)
  {
    let hs = stable.hard_state();
    let mut fsm = fsm;
    let mut applied = Index::ZERO;
    let mut poisoned = false;
    let mut poison_reason: Option<PoisonReason> = None;
    // Bootstrap tracker from the static seed first; may be overridden below if a
    // durable snapshot carries a more recent ConfState.
    let seed_cs = crate::ConfState::from_voters(config.voters().iter().copied());
    let mut tracker = crate::Tracker::from_conf_state(
      &seed_cs,
      Index::ZERO,
      config.max_inflight_msgs(),
      config.max_inflight_bytes(),
    );
    // Restore from a durable snapshot first: the compacted log no longer holds entries
    // <= meta.last_index, so the SM baseline comes from the snapshot; we then replay only
    // the durable post-snapshot committed tail.
    let snapshot = stable.snapshot();
    // The snapshot boundary `(N, last_term)` is captured before the blob is consumed below so the
    // log/snapshot boundary reconciliation can run afterward for both the present and absent cases.
    let snap_nt: Option<(Index, Term)> = snapshot
      .as_ref()
      .map(|(meta, _)| (meta.last_index(), meta.last_term()));
    // The LeaseGuard bound this snapshot carries over its compacted entries — combined below with a
    // scan of the live log to recompute `max_lease_window` from durable state alone.
    let snap_max_window: u64 = snapshot
      .as_ref()
      .map(|(meta, _)| meta.max_lease_window())
      .unwrap_or(0);
    if let Some((meta, data)) = snapshot {
      // Validate the durable snapshot's BOUNDARY before decoding/restoring the SM or installing the
      // tracker (which copies the ConfState verbatim) — a corrupt-on-disk or version-skewed snapshot
      // must fail-stop WITHOUT mutating the state machine, so every rejection sits ahead of
      // `decode`/`restore`. Mirrors the `on_install_snapshot` Step-0 gates (validate before any other
      // snapshot step). Two boundary faults are rejected here, each with its specific poison reason:
      //   1. Reserved-sentinel index: a snapshot whose `last_index` is the reserved
      //      sentinel u64::MAX is corrupt — a correct leader reserves it and never snapshots at
      //      it, and followers reject installing it. The boundary is unreadable by the half-open log
      //      ranges, so recovering into it would strand replay. Checked here, BEFORE `restore`, so the
      //      fail-stop is side-effect-free (the log-boundary sentinel is still checked below).
      //   2. Impossible membership: a ConfState that cannot represent a valid configuration.
      if meta.last_index().get() == u64::MAX {
        poisoned = true;
        poison_reason = Some(PoisonReason::LogExhausted);
      } else if !meta.conf().is_valid() {
        poisoned = true;
        poison_reason = Some(PoisonReason::InvalidConfState);
      } else {
        match <F::Snapshot as crate::Data>::decode_exact(data) {
          Ok(snap) => {
            if fsm.restore(snap).is_err() {
              poisoned = true;
              poison_reason = Some(PoisonReason::SnapshotRestore);
            } else {
              applied = meta.last_index();
              // Install the durable membership from the snapshot's ConfState.
              // This supersedes the bootstrap seed from Config.voters.
              // (Replaying ConfChange log entries to further refine membership is handled separately.)
              tracker = crate::Tracker::from_conf_state(
                &meta.conf().clone(),
                meta.last_index(),
                config.max_inflight_msgs(),
                config.max_inflight_bytes(),
              );
            }
          }
          Err(_) => {
            poisoned = true;
            poison_reason = Some(PoisonReason::SnapshotDecode);
          }
        }
      }
    }
    // Reserved-sentinel guard: a recovered durable LOG whose highest index is the reserved
    // sentinel u64::MAX is corrupt or version-skewed — a correct node never stores it (the leader
    // reserves it and followers reject importing it (the AppendEntries/InstallSnapshot guards)).
    // An entry at u64::MAX is unreadable by the half-open log ranges (apply/replication), so replay
    // would stall. Fail-stop rather than recover into it. (The SNAPSHOT-boundary sentinel is rejected
    // above, BEFORE `restore`, so its fail-stop never mutates the state machine.)
    if !poisoned && log.last_index().get() == u64::MAX {
      poisoned = true;
      poison_reason = Some(PoisonReason::LogExhausted);
    }
    // Reconcile the durable LOG boundary against the durable SNAPSHOT — for BOTH the snapshot-present
    // and snapshot-absent cases — enforcing ONE safety invariant: NEVER discard a committed entry
    // (committed `[1..=commit]` = `snapshot[1..=N] ++ log[N+1..=commit]`). The recovery action is
    // chosen by the pure, exhaustively case-tested `reconcile_restart_log`; here we only apply it.
    // Skipped if a snapshot step above already poisoned (e.g. corrupt blob, invalid ConfState).
    if !poisoned {
      // The highest committed index actually present in the log — the watermark that gates whether a
      // discard would lose committed data.
      let committed_in_log = core::cmp::min(hs.commit(), log.last_index());
      // Read the boundary term whenever the snapshot index `N` is MATERIALIZED in the log — either as
      // a live entry (`first_index <= N <= last_index`) or as the compacted baseline
      // (`first_index == N + 1`, i.e. `N == first_index - 1`, whose retained boundary term the log
      // exposes for AppendEntries consistency). Otherwise `N` is not in the log and its absence is
      // decided structurally. (`first_index <= N + 1` ⇔ `first_index <= n.next()`.)
      let boundary_term = snap_nt.and_then(|(n, _)| {
        if log.first_index() <= n.next() && n <= log.last_index() {
          Some(log.term(n).map_err(|_| ()))
        } else {
          None
        }
      });
      match reconcile_restart_log(
        snap_nt,
        committed_in_log,
        log.first_index(),
        log.last_index(),
        boundary_term,
      ) {
        RestartLogAction::None => {}
        RestartLogAction::Compact(n) => log.compact(n),
        RestartLogAction::Restore(n, term) => log.restore(n, term),
        RestartLogAction::Poison(reason) => {
          poisoned = true;
          poison_reason = Some(reason);
        }
      }
    }
    // Apply-time membership (etcd, spec §9): the recovered tracker is the snapshot's ConfState
    // baseline (set above). The COMMITTED tail beyond the snapshot is re-folded by the `apply_committed`
    // call at the end of `restart`, which replays `applied+1..=commit` and folds each committed
    // ConfChange exactly once. The UNCOMMITTED log tail (`commit+1..=last`) is NOT folded — the
    // configuration never reflects an uncommitted entry, so `conf_state()` always means the committed
    // voter set. A churn survivor whose removals are not yet committed campaigns on its committed
    // config and gets the removed-but-not-yet-committed peers' votes (the driver keeps them reachable
    // until their RemoveNode commits — see the membership driver contract in spec §9).
    // Never trust commit beyond the durable log; never below the snapshot baseline.
    let commit = core::cmp::min(hs.commit(), log.last_index()).max(applied);
    let read_only_opt = config.read_only();
    // Size the post-restart vote fence by the DURABLE PRE-CRASH PROMISE, not the
    // (possibly weaker) post-restart config, so this node cannot help elect a new leader inside a read-lease
    // it promised (as a follower) but has since forgotten (the in-memory `in_lease` state is lost on crash).
    // `reconcile_durable` is the pure, exhaustively case-tested lease-axis sibling of `reconcile_restart_log`:
    // it branches on the recovered PROVENANCE — a `Recorded` floor is authoritative (config drift cannot
    // shrink it; `assume_prior` is ignored — never over-fence a native node) while a legacy `Unrecorded`
    // record's prior promise is UNKNOWN and is fenced conservatively by this run's window + `assume_prior`.
    // SAFETY rests on `now >= every pre-crash ack instant` (follower local-clock monotonicity across
    // restart) — the same irreducible clock residual as all lease reads (see `lease_read_available`).
    let enforcing = config.check_quorum() || config.pre_vote();
    let recovered_floor = hs.promised_lease_support();
    let (lease_support_floor, lease_vote_fence_until) = match reconcile_durable(
      hs.lease_support(),
      enforcing,
      config.election_timeout(),
      assume_prior_lease_support,
    ) {
      LeaseReconcile::Ok(d) => (
        d.lease_support_floor,
        d.fence_window.map(|w| now.mono() + w),
      ),
      // A legacy `Unrecorded` record with no operator bound: fail-stop — the prior promise is
      // unbounded, so no finite fence is safe; recover via `restart_migrating(assume_prior = ..)`. A
      // poisoned node is inert (it emits nothing and persists nothing), so it can never grant a vote.
      LeaseReconcile::Poison => {
        if !poisoned {
          poisoned = true;
          poison_reason = Some(PoisonReason::LegacyLeaseUnrecoverable);
        }
        (None, None)
      }
    };
    // Recompute the self-describing LeaseGuard bound from DURABLE state — the snapshot's carried max
    // over compacted entries, plus a scan of the recovered live log. Derived from the durable log
    // (never a lagging in-memory or HardState value), so a successor's commit-wait always covers any
    // deposed leader's lease on a recovered entry. Skipped when already poisoned (an inert node never
    // leads); a scan read-fault fail-stops here rather than recover with a partial, under-sized bound.
    // `0` for non-LeaseGuard clusters (every `lease_window` is `0`).
    let recovered_max_lease_window = if poisoned {
      0
    } else {
      match Self::scan_max_lease_window(log) {
        Ok(m) => snap_max_window.max(m),
        Err(reason) => {
          poisoned = true;
          poison_reason = Some(reason);
          0
        }
      }
    };
    // Misconfiguration is handled by degradation, not rejection (see `Endpoint::new`); restart
    // construction stays infallible and identical across build profiles.
    let mut ep = Self {
      config,
      fsm,
      role: Role::Follower,
      term: hs.term(),
      voted_for: hs.vote(),
      leader: None,
      commit,
      applied,
      // Recovered commit is already durable in HardState — seed `committed_persisted` to it so
      // the handle_storage choke-point doesn't immediately re-persist an unchanged value.
      committed_persisted: commit,
      durable_index: log.last_index(),
      // volatile — after restart the reconciled durable log (Restore/Compact) already covers any
      // durable snapshot, so `durable_index` alone is the recoverable prefix; the gap this closes only
      // arises at RUNTIME from a dropped stale install.
      durable_snapshot_index: Index::ZERO,
      pending_install: None,
      prng: Prng::new(seed),
      votes: BTreeMap::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      // A restarted node recovers as Follower; the commit-wait is (re)computed at the next
      // `become_leader` from that election's `now` and the recovered `max_lease_window` below.
      commit_wait_until: None,
      max_lease_window: recovered_max_lease_window,
      // A restarted node comes up a fresh Follower with no pending lease-refresh demand.
      lease_refresh_wanted: false,
      // seed the op-id counter at seq 0 of THIS boot epoch (strictly greater than every prior
      // incarnation's ids), so a prior-incarnation storage completion that survives the crash can never
      // match a post-restart op (epoch-major OpId ordering + map-key equality make it miss every lookup
      // and every `>=` watermark check). The same boot_epoch namespaces forwarded-read tokens below.
      next_op_id: crate::OpId::first_of_epoch(boot_epoch),
      pending: BTreeMap::new(),
      inflight_append_upto: BTreeMap::new(),
      poisoned,
      poison_reason,
      pending_compact: None,
      snapshot_resend_after: BTreeMap::new(),
      // the recovered `hs.term()` came from durable HardState, so it IS durable. Seed both
      // `durable_term` and `last_submitted_term` to it so `term_is_durable()` is true immediately after
      // restart and follower acks are not spuriously deferred.
      durable_term: hs.term(),
      last_submitted_term: hs.term(),
      term_persist_opid: crate::OpId::ZERO,
      // `recovered_floor` (= hs.lease_support()) is what is durable NOW; `lease_support_floor` may be
      // larger (a config grow this incarnation), in which case the post-construction step below persists it
      // and the advertise gate holds at ZERO until that write drains. On a same/shrunk-config restart the
      // floor already equals the recovered value, so the gate is true immediately (no advertise stall).
      lease_support_floor,
      last_submitted_lease_support: recovered_floor,
      durable_lease_support: recovered_floor,
      lease_support_persist_opid: crate::OpId::ZERO,
      term_gated_append_ack: None,
      term_gated_snapshot_ack: None,
      // On restart, ZERO is acceptable — see the field-level comment on pending_conf_index.
      pending_conf_index: Index::ZERO,
      tracker,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      read_only: ReadOnly::new(read_only_opt),
      pending_reads: std::vec::Vec::new(),
      forwarded_reads: ForwardedReads::new(boot_epoch),
      lease_round: 0,
      lease_round_start: now.mono(),
      lease_acks: BTreeSet::new(),
      lease_min_support: core::time::Duration::ZERO,
      lease_valid_until: None,
      lease_vote_fence_until,
      // A restarted node is not leader (recovers as Follower) and has authorized no handoff.
      forced_handoff_this_term: false,
      lead_transferee: None,
      transfer_deadline: None,
    };
    // Replay the durable committed tail (applied..commit] into the restored SM. Skip if the
    // snapshot restore failed (the SM is in an unknown state and the node is poisoned).
    if !ep.poisoned {
      ep.apply_committed(log);
    }
    // if this incarnation's enforcement window GREW the durable floor (a config grow, or a legacy
    // record being recorded for the first time under an enforcing config), persist the raised floor ONCE
    // here. `submit_write` records the watermark so the advertise gate holds at ZERO until it drains. On a
    // same/shrunk-config restart the floor already equals the recovered value, so this is a no-op (no
    // write, no advertise stall). Skipped when poisoned (no side effects from a poisoned restart).
    if !ep.poisoned && ep.lease_support_floor > ep.durable_lease_support {
      let opid = ep.mint_op_id();
      // The `submit_write` choke-point `raise`s `ep.lease_support_floor` onto this write, recording the
      // floor AND upgrading a legacy `Unrecorded` recovered record to `Recorded` (the self-heal).
      let hsw = stable.hard_state();
      ep.submit_write(stable, opid, hsw);
    }
    ep.events.clear();
    ep.arm_election_timer(now);
    ep
  }

  /// Scan the durable live log `[first_index ..= last_index]` for the MAX LeaseGuard `lease_window`
  /// — the restart recompute of `max_lease_window`, combined with the restored snapshot's carried max
  /// (compacted entries no longer in the live log). Derived from durable state, not a lagging
  /// in-memory/HardState value, so a successor's commit-wait always covers any deposed lease on a
  /// recovered entry. Bounded by the live log (≤ `snapshot_threshold`).
  ///
  /// FAIL-STOP on any read fault: an `entries` error — or an unexpectedly empty chunk for a non-empty
  /// in-range read — is fatal, NOT end-of-scan. Stopping early could miss a larger inherited window
  /// in the durable suffix and let a successor under-wait (a stale read), so the caller poisons
  /// rather than constructing a live endpoint with a partial bound.
  fn scan_max_lease_window<L: LogStore>(log: &L) -> Result<u64, PoisonReason> {
    let last = log.last_index();
    let mut idx = log.first_index();
    let mut max = 0u64;
    while idx <= last {
      let chunk = match log.entries(idx..last.next(), 1 << 20) {
        Ok(c) if !c.is_empty() => c,
        _ => return Err(PoisonReason::LogRead),
      };
      for e in chunk {
        max = max.max(e.lease_window());
      }
      // `entries` may return a prefix of the requested range; advance past the last entry it gave
      // (always `Some` — the chunk is non-empty — so the `ok_or` is a defensive fail-stop).
      idx = chunk
        .last()
        .map(|e| e.index().next())
        .ok_or(PoisonReason::LogRead)?;
    }
    Ok(max)
  }
}
