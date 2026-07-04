use core::time::Duration;

use sailing_driver::GroupBlueprint;
use sailing_proto::Config;

use super::blueprint_names;

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// The plain bootstrap shape: the seed voters authorize exactly themselves — a peer outside the
/// list is refused even though the group id and the blueprint itself are valid.
#[test]
fn a_blueprint_names_exactly_its_seed_voters() {
  let blueprint = GroupBlueprint::new(
    Config::try_new(2u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
    0,
  );
  assert!(blueprint_names(&blueprint, &1));
  assert!(blueprint_names(&blueprint, &2));
  assert!(
    !blueprint_names(&blueprint, &3),
    "a solicitor the seed config does not name is refused"
  );
}

/// The learner-join (observer) shape: the joining HOST's own id is absent from the seed voters
/// by construction (`try_new_observer`), and that own id never authorizes a remote solicitor —
/// only the voter list does, which names the soliciting leader.
#[test]
fn an_observer_seed_names_the_remote_voters_not_its_own_id() {
  let blueprint = GroupBlueprint::new(
    Config::try_new_observer(3u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
    0,
  );
  assert!(
    blueprint_names(&blueprint, &1),
    "the soliciting leader is named by the seed voters"
  );
  assert!(
    !blueprint_names(&blueprint, &3),
    "the config's own id is the HOST identity, not solicitor authorization"
  );
}
