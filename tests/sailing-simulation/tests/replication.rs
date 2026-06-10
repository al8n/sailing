#![allow(missing_docs)]
use sailing_simulation::Cluster;

#[test]
fn proposals_replicate_and_agree() {
  let mut c = Cluster::new(3);
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "a leader should emerge within 100 steps"
  );
  for i in 0..10u32 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed on the leader"
    );
    c.run_until(50, |_| false); // let it replicate + commit
  }
  // every node converges to the same applied sequence of at least 10 entries
  assert!(
    c.run_until(200, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "cluster must agree on >= 10 applied entries"
  );
}

#[test]
fn agreement_survives_leader_isolation() {
  let mut c = Cluster::new(3);
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "initial leader must emerge"
  );
  c.propose(b"before").unwrap();
  c.run_until(50, |_| false);
  let old = c.leader().unwrap();
  c.isolate(old);
  assert!(
    c.run_until(300, |c| c.leader().is_some_and(|l| l != old)),
    "a new leader must emerge after isolating the old one"
  );
  c.propose(b"after").unwrap();
  c.run_until(100, |_| false);
  assert!(
    c.agreement_holds(),
    "agreement must hold after leader isolation"
  );
}
