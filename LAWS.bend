import Base
import ./verification/bridge.bend as Bridge

law wrap_waits_for_all_confirmations:
  for remaining: Nat
  {Bridge.wrap_is_final(1n+remaining) == False{} : Bool}

law final_wrap_is_eligible:
  {Bridge.wrap_is_final(0n) == True{} : Bool}

# Queued EVM events wait for the required depth, even with a good receipt.
law immature_event_waits:
  for receipt: Bridge.Receipt
  for block: Bridge.Canonical
  {Bridge.confirm(False{}, receipt, block) == Bridge.Retry{} : Bridge.Decision}

law receipt_error_waits:
  for block: Bridge.Canonical
  {Bridge.confirm(True{}, Bridge.ReadError{}, block) == Bridge.Retry{} : Bridge.Decision}

law successful_final_receipt_confirms:
  for block: Bridge.Canonical
  {Bridge.confirm(True{}, Bridge.SuccessfulInBlock{}, block) == Bridge.Confirm{} : Bridge.Decision}

law uncertain_canonical_block_waits:
  {Bridge.confirm(True{}, Bridge.NeedsBlockCheck{}, Bridge.Uncertain{}) == Bridge.Retry{} : Bridge.Decision}

law missing_canonical_log_waits:
  {Bridge.confirm(True{}, Bridge.NeedsBlockCheck{}, Bridge.ObservedWithoutLog{}) == Bridge.Retry{} : Bridge.Decision}

law matching_canonical_log_confirms:
  {Bridge.confirm(True{}, Bridge.NeedsBlockCheck{}, Bridge.ObservedWithLog{}) == Bridge.Confirm{} : Bridge.Decision}

law agreed_replacement_requires_recheck:
  {Bridge.confirm(True{}, Bridge.NeedsBlockCheck{}, Bridge.Replacement{}) == Bridge.RecheckReplacement{} : Bridge.Decision}

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
