import Base
import ./verification/bridge.bend as Bridge
import ./LAWS.bend as Laws

def Laws.wrap_waits_for_all_confirmations(remaining):
  {==}

def Laws.final_wrap_is_eligible():
  {==}

def Laws.immature_event_waits(block):
  {==}

def Laws.mismatched_transfer_waits():
  {==}

def Laws.uncertain_canonical_block_waits():
  {==}

def Laws.missing_canonical_log_waits():
  {==}

def Laws.matching_canonical_log_confirms():
  {==}

def Laws.agreed_replacement_requires_recheck():
  {==}

def Laws.signed_survives_backfill(unredeemed, same_block):
  {==}

def Laws.reconciled_survives_backfill(signed, same_block):
  match signed:
    case False{}: {==}
    case True{}: {==}

def Laws.refreshed_block_survives_backfill(signed, unredeemed):
  match signed unredeemed:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} _: {==}

def Laws.stale_unsigned_record_can_be_removed():
  {==}

def Laws.unverified_bridge_view_aborts(pool):
  {==}

def Laws.stable_bridge_view_preserves_pool(pool):
  {==}

def Laws.full_pool_ignores_tail(tail, budget):
  {==}

def Laws.exhausted_input_completes(slots, budget):
  match slots budget:
    case 0n _: {==}
    case 1n+s 0n: {==}
    case 1n+s 1n+b: {==}

def Laws.exhausted_budget_with_work_left_skips_signing(head, tail, slots):
  {==}

def Laws.required_lookup_or_write_failure_aborts(tail, slots, budget):
  {==}

def Laws.known_request_costs_one_lookup_and_no_slot(tail, slots, budget):
  {==}

def Laws.missing_requests_keep_storage_order(ids, tail):
  match ids:
    case Nil{}: {==}
    case Con{id, rest}:
      %Laws.missing_requests_keep_storage_order(rest, tail) : {Bridge.include(id, Bridge.signing_pool(List.length(&1, Nat, rest), List.length(&1, Nat, rest), Bridge.missing_prefix(rest, tail))) == Bridge.include(id, _) : Bridge.Pool}
      {==}

def Laws.partial_candidates_cannot_override_failure(id):
  {==}

def Laws.partial_candidates_cannot_override_budget(id):
  {==}

def Laws.scan_respects_block_limit(available, limit):
  match available limit:
    case 0n 0n: {==}
    case 0n 1n+l: {==}
    case 1n+a 0n: {==}
    case 1n+a 1n+l: Laws.scan_respects_block_limit(a, l)

def Laws.scan_stops_at_head(available, limit):
  match available limit:
    case 0n _: {==}
    case 1n+a 0n: {==}
    case 1n+a 1n+l: Laws.scan_stops_at_head(a, l)

def Laws.one_block_limit_makes_progress(more):
  match more:
    case 0n: {==}
    case 1n+m: {==}

def Laws.competing_pools_cannot_both_outnumber_each_other(a, b):
  match a b:
    case 0n 0n: {==}
    case 0n 1n+y: {==}
    case 1n+x 0n: {==}
    case 1n+x 1n+y: Laws.competing_pools_cannot_both_outnumber_each_other(x, y)

def Laws.one_to_one_pool_split_abstains():
  {==}

def Laws.minority_pool_with_one_of_three_abstains():
  {==}

def Laws.matching_pool_with_two_of_three_can_proceed():
  {==}

def Laws.redeemed_survives_pending_reconciliation():
  {==}

def Laws.revoked_survives_pending_reconciliation():
  {==}

def Laws.invalid_scan_preserves_checkpoint(previous, last):
  {==}

def Laws.valid_scan_commits_through_last_block(previous, last):
  {==}

def Laws.emergency_blocks_live_transition(redeem_halt_pending, restart_required):
  match redeem_halt_pending restart_required:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} False{}: {==}
    case True{} True{}: {==}

def Laws.emergency_blocks_keygen_transition(redeem_halt_pending, restart_required):
  match redeem_halt_pending restart_required:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} False{}: {==}
    case True{} True{}: {==}

def Laws.emergency_blocks_resign_transition(redeem_halt_pending, restart_required):
  match redeem_halt_pending restart_required:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} False{}: {==}
    case True{} True{}: {==}

def Laws.redeem_halt_blocks_transfer_signing(state, restart_required):
  match state:
    case Bridge.StateLive{}: {==}
    case Bridge.StateKeyGen{}: {==}
    case Bridge.StateHalted{}: {==}
    case Bridge.StateEmergency{}: {==}
    case Bridge.StateReSign{}: {==}

def Laws.restart_latch_blocks_transfer_signing(state, redeem_halt_pending):
  match state redeem_halt_pending:
    case Bridge.StateLive{} False{}: {==}
    case Bridge.StateLive{} True{}: {==}
    case Bridge.StateKeyGen{} False{}: {==}
    case Bridge.StateKeyGen{} True{}: {==}
    case Bridge.StateHalted{} False{}: {==}
    case Bridge.StateHalted{} True{}: {==}
    case Bridge.StateEmergency{} False{}: {==}
    case Bridge.StateEmergency{} True{}: {==}
    case Bridge.StateReSign{} False{}: {==}
    case Bridge.StateReSign{} True{}: {==}

def Laws.restart_latch_blocks_live_transition(current, redeem_halt_pending):
  match current redeem_halt_pending:
    case Bridge.StateLive{} False{}: {==}
    case Bridge.StateLive{} True{}: {==}
    case Bridge.StateKeyGen{} False{}: {==}
    case Bridge.StateKeyGen{} True{}: {==}
    case Bridge.StateHalted{} False{}: {==}
    case Bridge.StateHalted{} True{}: {==}
    case Bridge.StateEmergency{} False{}: {==}
    case Bridge.StateEmergency{} True{}: {==}
    case Bridge.StateReSign{} False{}: {==}
    case Bridge.StateReSign{} True{}: {==}

def Laws.halted_never_signs_transfers(redeem_halt_pending, restart_required):
  match redeem_halt_pending restart_required:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} False{}: {==}
    case True{} True{}: {==}

def Laws.halt_signing_leaves_protected_or_halts(state):
  match state:
    case Bridge.StateLive{}: {==}
    case Bridge.StateKeyGen{}: {==}
    case Bridge.StateHalted{}: {==}
    case Bridge.StateEmergency{}: {==}
    case Bridge.StateReSign{}: {==}

def Laws.interrupted_resign_promotes_on_live_resume():
  {==}

def Laws.empty_endpoints_never_agree():
  {==}

def Laws.leading_endpoint_error_blocks_consensus(rest):
  {==}

def Laws.trailing_endpoint_error_blocks_consensus(expected, rest):
  {==}

def nat_eq_refl(+n: Nat) -> {True{} == Nat.is_eq(n, n) : Bool}:
  match n:
    case 0n: {==}
    case 1n+p: nat_eq_refl(p)

def nat_eq_add_succ_right(+n: Nat, delta: Nat) -> {False{} == Nat.is_eq(n, Nat.add(n, 1n+delta)) : Bool}:
  match n:
    case 0n: {==}
    case 1n+p: nat_eq_add_succ_right(p, delta)

def nat_eq_add_succ_left(+n: Nat, delta: Nat) -> {False{} == Nat.is_eq(Nat.add(n, 1n+delta), n) : Bool}:
  match n:
    case 0n: {==}
    case 1n+p: nat_eq_add_succ_left(p, delta)

def Laws.unanimous_pair_agrees(h):
  %nat_eq_refl(h) : {Bridge.keep_consensus(_, Bridge.ConsensusAgreed{h}) == Bridge.ConsensusAgreed{h} : Bridge.Consensus}
  {==}

def Laws.byzantine_higher_hash_blocks_consensus(h, delta):
  %nat_eq_add_succ_right(h, delta) : {Bridge.keep_consensus(_, Bridge.ConsensusAgreed{h}) == Bridge.ConsensusNone{} : Bridge.Consensus}
  {==}

def Laws.byzantine_lower_hash_blocks_consensus(h, delta):
  %nat_eq_add_succ_left(h, delta) : {Bridge.keep_consensus(_, Bridge.ConsensusAgreed{Nat.add(h, 1n+delta)}) == Bridge.ConsensusNone{} : Bridge.Consensus}
  {==}

def Laws.zero_window_size_rejects_vote(pool_nonempty, prior, candidate):
  {==}

def Laws.empty_pool_rejects_vote(size, prior, candidate):
  match size:
    case 0n: {==}
    case 1n+s: {==}

def Laws.first_vote_records_candidate(size, candidate):
  {==}

def Laws.same_vote_retry_is_idempotent(size, digest):
  %nat_eq_refl(digest) : {Bridge.vote_match(_, digest) == Bridge.VoteAccepted{digest} : Bridge.VoteOutcome}
  {==}

def Laws.higher_conflicting_vote_is_rejected(size, digest, delta):
  %nat_eq_add_succ_right(digest, delta) : {Bridge.vote_match(_, digest) == Bridge.VoteRejected{} : Bridge.VoteOutcome}
  {==}

def Laws.lower_conflicting_vote_is_rejected(size, digest, delta):
  %nat_eq_add_succ_left(digest, delta) : {Bridge.vote_match(_, Nat.add(digest, 1n+delta)) == Bridge.VoteRejected{} : Bridge.VoteOutcome}
  {==}

def Laws.matched_live_redeem_never_halts(acknowledged, evidence_ok, settled_latest, settled_final):
  {==}

def Laws.unmatched_redeem_without_agreed_evidence_retries(live, acknowledged, settled_latest, settled_final):
  match live acknowledged:
    case False{} False{}: {==}
    case False{} True{}: {==}
    case True{} False{}: {==}
    case True{} True{}: {==}

def Laws.acknowledged_unmatched_redeem_archives(live, settled_latest, settled_final):
  match live:
    case False{}: {==}
    case True{}: {==}

def Laws.live_unmatched_unsettled_redeem_halts(settled_final):
  {==}

def Laws.live_settled_redeem_waits_for_finality(settled_final):
  {==}

def Laws.historical_settled_redeem_archives_without_halt(settled_latest):
  {==}

def Laws.historical_unsettled_unmatched_redeem_halts(settled_latest):
  {==}
