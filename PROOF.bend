import Base
import ./verification/bridge.bend as Bridge
import ./LAWS.bend as Laws

def Laws.wrap_waits_for_all_confirmations(remaining):
  {==}

def Laws.final_wrap_is_eligible():
  {==}

def Laws.immature_event_waits(receipt, block):
  {==}

def Laws.receipt_error_waits(block):
  {==}

def Laws.successful_final_receipt_confirms(block):
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
