#![allow(missing_docs)]
use sailing_simulation::Cluster;

#[test]
fn three_node_cluster_elects_exactly_one_leader() {
  let mut c = Cluster::new(3);
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "a leader should emerge within 100 steps"
  );
  assert_eq!(c.leader_count(), 1);
}

#[test]
fn five_node_cluster_elects_one_leader() {
  let mut c = Cluster::new(5);
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  assert_eq!(c.leader_count(), 1);
}

#[test]
fn leader_is_stable_once_elected() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(100, |c| c.leader_count() == 1));
  let leader = c.leader().unwrap();
  // keep ticking; the same node stays leader (heartbeats suppress re-elections)
  for _ in 0..200 {
    c.tick();
  }
  assert_eq!(c.leader(), Some(leader));
  assert_eq!(c.leader_count(), 1);
}

#[test]
fn isolating_the_leader_elects_a_new_one() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(100, |c| c.leader_count() == 1));
  let old = c.leader().unwrap();
  c.isolate(old);
  // the remaining majority must elect a new leader in a higher term
  assert!(
    c.run_until(300, |c| c.leader().is_some_and(|l| l != old)),
    "a new leader should emerge among the majority"
  );
  let new_leader = c.leader().unwrap();
  assert_ne!(new_leader, old);
}
