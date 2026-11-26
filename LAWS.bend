import Base
import ./verification/bridge.bend as Bridge

law wrap_waits_for_all_confirmations:
  for remaining: Nat
  {Bridge.wrap_is_final(1n+remaining) == False{} : Bool}

law final_wrap_is_eligible:
  {Bridge.wrap_is_final(0n) == True{} : Bool}

# Both live and historical unwraps require finality and an exact source event.
law immature_event_waits:
  for block: Bridge.Canonical
  {Bridge.confirm(False{}, block) == Bridge.Retry{} : Bridge.Decision}

law uncertain_canonical_block_waits:
  {Bridge.confirm(True{}, Bridge.Uncertain{}) == Bridge.Retry{} : Bridge.Decision}

law missing_canonical_log_waits:
  {Bridge.confirm(True{}, Bridge.ObservedWithoutLog{}) == Bridge.Retry{} : Bridge.Decision}

law mismatched_transfer_waits:
  {Bridge.confirm(True{}, Bridge.ObservedMismatch{}) == Bridge.Retry{} : Bridge.Decision}

law matching_canonical_log_confirms:
  {Bridge.confirm(True{}, Bridge.ObservedWithLog{}) == Bridge.Confirm{} : Bridge.Decision}

law agreed_replacement_requires_recheck:
  {Bridge.confirm(True{}, Bridge.Replacement{}) == Bridge.RecheckReplacement{} : Bridge.Decision}

# Backfill must preserve records signed, reconciled or refreshed since inspection.
law signed_survives_backfill:
  for unredeemed: Bool
  for same_block: Bool
  {Bridge.can_delete(True{}, unredeemed, same_block) == False{} : Bool}

law reconciled_survives_backfill:
  for signed: Bool
  for same_block: Bool
  {Bridge.can_delete(signed, False{}, same_block) == False{} : Bool}

law refreshed_block_survives_backfill:
  for signed: Bool
  for unredeemed: Bool
  {Bridge.can_delete(signed, unredeemed, False{}) == False{} : Bool}

law stale_unsigned_record_can_be_removed:
  {Bridge.can_delete(False{}, True{}, True{}) == True{} : Bool}

# Signing consumes a bounded prefix. These laws quantify over every possible
# remaining sequence of Zenon answers, including transport and storage errors.
law unverified_bridge_view_aborts:
  for pool: Bridge.Pool
  {Bridge.checked_pool(False{}, pool) == Bridge.PoolAborted{} : Bridge.Pool}

law stable_bridge_view_preserves_pool:
  for pool: Bridge.Pool
  {Bridge.checked_pool(True{}, pool) == pool : Bridge.Pool}

law full_pool_ignores_tail:
  for tail: List<Bridge.Lookup>
  for budget: Nat
  {Bridge.signing_pool(0n, budget, tail) == Bridge.PoolReady{[]} : Bridge.Pool}

law exhausted_input_completes:
  for slots: Nat
  for budget: Nat
  {Bridge.signing_pool(slots, budget, []) == Bridge.PoolReady{[]} : Bridge.Pool}

law exhausted_budget_with_work_left_skips_signing:
  for head: Bridge.Lookup
  for tail: List<Bridge.Lookup>
  for slots: Nat
  {Bridge.signing_pool(1n+slots, 0n, head <> tail) == Bridge.PoolIncomplete{} : Bridge.Pool}

law required_lookup_or_write_failure_aborts:
  for tail: List<Bridge.Lookup>
  for slots: Nat
  for budget: Nat
  {Bridge.signing_pool(1n+slots, 1n+budget, Bridge.Failed{} <> tail) == Bridge.PoolAborted{} : Bridge.Pool}

law known_request_costs_one_lookup_and_no_slot:
  for tail: List<Bridge.Lookup>
  for +slots: Nat
  for +budget: Nat
  {Bridge.signing_pool(1n+slots, 1n+budget, Bridge.Known{} <> tail) == Bridge.signing_pool(1n+slots, budget, tail) : Bridge.Pool}

law missing_requests_keep_storage_order:
  for ids: List<Nat>
  for tail: List<Bridge.Lookup>
  count = List.length(&1, Nat, ids)
  {Bridge.signing_pool(count, count, Bridge.missing_prefix(ids, tail)) == Bridge.PoolReady{ids} : Bridge.Pool}

law partial_candidates_cannot_override_failure:
  for id: Nat
  {Bridge.include(id, Bridge.PoolAborted{}) == Bridge.PoolAborted{} : Bridge.Pool}

law partial_candidates_cannot_override_budget:
  for id: Nat
  {Bridge.include(id, Bridge.PoolIncomplete{}) == Bridge.PoolIncomplete{} : Bridge.Pool}

# A provider's advertised block count applies even inside the finality window.
law scan_respects_block_limit:
  for available: Nat
  for limit: Nat
  {Nat.is_le(Bridge.scan_count(available, limit), limit) == True{} : Bool}

law scan_stops_at_head:
  for available: Nat
  for limit: Nat
  {Nat.is_le(Bridge.scan_count(available, limit), available) == True{} : Bool}

law one_block_limit_makes_progress:
  for more: Nat
  {Bridge.scan_count(1n+more, 1n) == 1n : Nat}

law competing_pools_cannot_both_outnumber_each_other:
  for +a: Nat
  for +b: Nat
  {Bridge.outnumbers(a, b) && Bridge.outnumbers(b, a) == False{} : Bool}

# Ordered pools from the same ceremony on two different stable views.
law one_to_one_pool_split_abstains:
  {Bridge.can_certify(1n, 1n) == False{} : Bool}

law minority_pool_with_one_of_three_abstains:
  {Bridge.can_certify(1n, 2n) == False{} : Bool}

law matching_pool_with_two_of_three_can_proceed:
  {Bridge.can_certify(2n, 1n) == True{} : Bool}

law redeemed_survives_pending_reconciliation:
  {Bridge.apply_pending(Bridge.Redeemed{}) == Bridge.Redeemed{} : Bridge.UnwrapStatus}

law revoked_survives_pending_reconciliation:
  {Bridge.apply_pending(Bridge.Revoked{}) == Bridge.Revoked{} : Bridge.UnwrapStatus}

law invalid_scan_preserves_checkpoint:
  for previous: Nat
  for last: Nat
  {Bridge.scan_checkpoint(previous, last, False{}) == previous : Nat}

law valid_scan_commits_through_last_block:
  for previous: Nat
  for +last: Nat
  {Bridge.scan_checkpoint(previous, last, True{}) == last : Nat}

# GlobalState safety, emergency halt ownership, and restart-required latch.
law emergency_blocks_live_transition:
  for redeem_halt_pending: Bool
  for restart_required: Bool
  {Bridge.transition_allowed(Bridge.StateEmergency{}, Bridge.StateLive{}, redeem_halt_pending, restart_required) == False{} : Bool}

law emergency_blocks_keygen_transition:
  for redeem_halt_pending: Bool
  for restart_required: Bool
  {Bridge.transition_allowed(Bridge.StateEmergency{}, Bridge.StateKeyGen{}, redeem_halt_pending, restart_required) == False{} : Bool}

law emergency_blocks_resign_transition:
  for redeem_halt_pending: Bool
  for restart_required: Bool
  {Bridge.transition_allowed(Bridge.StateEmergency{}, Bridge.StateReSign{}, redeem_halt_pending, restart_required) == False{} : Bool}

law redeem_halt_blocks_transfer_signing:
  for state: Bridge.BridgeState
  for restart_required: Bool
  {Bridge.can_sign_transfers(state, True{}, restart_required) == False{} : Bool}

law restart_latch_blocks_transfer_signing:
  for state: Bridge.BridgeState
  for redeem_halt_pending: Bool
  {Bridge.can_sign_transfers(state, redeem_halt_pending, True{}) == False{} : Bool}

law restart_latch_blocks_live_transition:
  for current: Bridge.BridgeState
  for redeem_halt_pending: Bool
  {Bridge.transition_allowed(current, Bridge.StateLive{}, redeem_halt_pending, True{}) == False{} : Bool}

law halted_never_signs_transfers:
  for redeem_halt_pending: Bool
  for restart_required: Bool
  {Bridge.can_sign_transfers(Bridge.StateHalted{}, redeem_halt_pending, restart_required) == False{} : Bool}

law halt_signing_leaves_protected_or_halts:
  for state: Bridge.BridgeState
  {Bridge.is_protected_state(Bridge.halt_signing_target(state)) == True{} : Bool}

law interrupted_resign_promotes_on_live_resume:
  {Bridge.next_state(Bridge.StateHalted{}, Bridge.StateLive{}, False{}, False{}, True{}) == Bridge.StateReSign{} : Bridge.BridgeState}

# Multi-endpoint unanimous agreement (Byzantine EVM RPC resistance).
law empty_endpoints_never_agree:
  {Bridge.agree_all([]) == Bridge.ConsensusNone{} : Bridge.Consensus}

law leading_endpoint_error_blocks_consensus:
  for rest: List<Bridge.EndpointAnswer>
  {Bridge.agree_all(Bridge.AnswerError{} <> rest) == Bridge.ConsensusNone{} : Bridge.Consensus}

law trailing_endpoint_error_blocks_consensus:
  for expected: Nat
  for rest: List<Bridge.EndpointAnswer>
  {Bridge.agree_rest(expected, Bridge.AnswerError{} <> rest) == Bridge.ConsensusNone{} : Bridge.Consensus}

law unanimous_pair_agrees:
  for +h: Nat
  {Bridge.agree_all([Bridge.AnswerHash{h}, Bridge.AnswerHash{h}]) == Bridge.ConsensusAgreed{h} : Bridge.Consensus}

law byzantine_higher_hash_blocks_consensus:
  for +h: Nat
  for delta: Nat
  {Bridge.agree_all([Bridge.AnswerHash{h}, Bridge.AnswerHash{Nat.add(h, 1n+delta)}]) == Bridge.ConsensusNone{} : Bridge.Consensus}

law byzantine_lower_hash_blocks_consensus:
  for +h: Nat
  for delta: Nat
  {Bridge.agree_all([Bridge.AnswerHash{Nat.add(h, 1n+delta)}, Bridge.AnswerHash{h}]) == Bridge.ConsensusNone{} : Bridge.Consensus}

# Unwrap pool agreement vote non-equivocation.
law zero_window_size_rejects_vote:
  for pool_nonempty: Bool
  for prior: Bridge.VoteRecord
  for candidate: Nat
  {Bridge.reserve_vote(0n, pool_nonempty, prior, candidate) == Bridge.VoteRejected{} : Bridge.VoteOutcome}

law empty_pool_rejects_vote:
  for size: Nat
  for prior: Bridge.VoteRecord
  for candidate: Nat
  {Bridge.reserve_vote(size, False{}, prior, candidate) == Bridge.VoteRejected{} : Bridge.VoteOutcome}

law first_vote_records_candidate:
  for size: Nat
  for +candidate: Nat
  {Bridge.reserve_vote(1n+size, True{}, Bridge.NoVote{}, candidate) == Bridge.VoteAccepted{candidate} : Bridge.VoteOutcome}

law same_vote_retry_is_idempotent:
  for size: Nat
  for +digest: Nat
  {Bridge.reserve_vote(1n+size, True{}, Bridge.Voted{digest}, digest) == Bridge.VoteAccepted{digest} : Bridge.VoteOutcome}

law higher_conflicting_vote_is_rejected:
  for size: Nat
  for +digest: Nat
  for delta: Nat
  {Bridge.reserve_vote(1n+size, True{}, Bridge.Voted{digest}, Nat.add(digest, 1n+delta)) == Bridge.VoteRejected{} : Bridge.VoteOutcome}

law lower_conflicting_vote_is_rejected:
  for size: Nat
  for +digest: Nat
  for delta: Nat
  {Bridge.reserve_vote(1n+size, True{}, Bridge.Voted{Nat.add(digest, 1n+delta)}, digest) == Bridge.VoteRejected{} : Bridge.VoteOutcome}

# Unmatched RegisteredRedeem fraud-detection decision laws.
law matched_live_redeem_never_halts:
  for acknowledged: Bool
  for evidence_ok: Bool
  for settled_latest: Bool
  for settled_final: Bool
  {Bridge.decide_redeem(True{}, True{}, acknowledged, evidence_ok, settled_latest, settled_final) == Bridge.RedeemRecordMatch{} : Bridge.RedeemAction}

law unmatched_redeem_without_agreed_evidence_retries:
  for live: Bool
  for acknowledged: Bool
  for settled_latest: Bool
  for settled_final: Bool
  {Bridge.decide_redeem(False{}, live, acknowledged, False{}, settled_latest, settled_final) == Bridge.RedeemRetry{} : Bridge.RedeemAction}

law acknowledged_unmatched_redeem_archives:
  for live: Bool
  for settled_latest: Bool
  for settled_final: Bool
  {Bridge.decide_redeem(False{}, live, True{}, True{}, settled_latest, settled_final) == Bridge.RedeemArchive{} : Bridge.RedeemAction}

law live_unmatched_unsettled_redeem_halts:
  for settled_final: Bool
  {Bridge.decide_redeem(False{}, True{}, False{}, True{}, False{}, settled_final) == Bridge.RedeemHalt{} : Bridge.RedeemAction}

law live_settled_redeem_waits_for_finality:
  for settled_final: Bool
  {Bridge.decide_redeem(False{}, True{}, False{}, True{}, True{}, settled_final) == Bridge.RedeemRetry{} : Bridge.RedeemAction}

law historical_settled_redeem_archives_without_halt:
  for settled_latest: Bool
  {Bridge.decide_redeem(False{}, False{}, False{}, True{}, settled_latest, True{}) == Bridge.RedeemArchive{} : Bridge.RedeemAction}

law historical_unsettled_unmatched_redeem_halts:
  for settled_latest: Bool
  {Bridge.decide_redeem(False{}, False{}, False{}, True{}, settled_latest, False{}) == Bridge.RedeemHalt{} : Bridge.RedeemAction}
