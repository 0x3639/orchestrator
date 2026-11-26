import Base

# network/manager.go selects ordinary unsigned wraps in Zenon RPC order.
# The RPC reports confirmations still required, rather than confirmations seen.
def wrap_is_final(remaining: Nat) -> Bool:
  match remaining:
    case 0n: True{}
    case 1n+r: False{}

# The bridge requires a source-side lock or burn for every redeem:
# https://hypercore-team.github.io/security.html. ObservedWithLog means the
# canonical bridge log identifies this transfer and yields its exact signed fields.
type Canonical is Data:
  Uncertain{}
  Replacement{}
  ObservedWithLog{}
  ObservedWithoutLog{}
  ObservedMismatch{}

type Decision is Data:
  Retry{}
  Confirm{}
  RecheckReplacement{}

def confirm(mature: Bool, block: Canonical) -> Decision:
  match mature block:
    case False{} _: Retry{}
    case True{} Replacement{}: RecheckReplacement{}
    case True{} ObservedWithLog{}: Confirm{}
    case True{} _: Retry{}

# db/unwrap/entries.go re-reads these three predicates under the storage lock
# after the caller establishes that the previously observed block was replaced.
def can_delete(signed: Bool, unredeemed: Bool, same_block: Bool) -> Bool:
  match signed:
    case True{}: False{}
    case False{}: unredeemed && same_block

# node/reconcile.go. Each input is the result for a request in storage order.
# Failed includes a lookup error or failure to persist a known request's status.
type Lookup is Data:
  Missing{id: Nat}
  Known{}
  Failed{}

type Pool is Type:
  PoolReady{ids: List<Nat>}
  PoolIncomplete{}
  PoolAborted{}

# Each nonempty scan must observe the same bridge account block at both ends.
# Failure to establish that view discards the entire decision.
def checked_pool(same_frontier: Bool, pool: Pool) -> Pool:
  match same_frontier:
    case False{}: PoolAborted{}
    case True{}: pool

def include(id: Nat, result: Pool) -> Pool:
  match result:
    case PoolReady{ids}: PoolReady{id <> ids}
    case PoolIncomplete{}: PoolIncomplete{}
    case PoolAborted{}: PoolAborted{}

# Only a complete pool reaches signing. The Go implementation also returns a
# diagnostic prefix when incomplete. That prefix is never proposed to peers.
def signing_pool(slots: Nat, budget: Nat, xs: List<Lookup>) -> Pool:
  match slots budget xs:
    case 0n _ _: PoolReady{[]}
    case 1n+s 0n Nil{}: PoolReady{[]}
    case 1n+s 0n Con{_, _}: PoolIncomplete{}
    case 1n+s 1n+b Nil{}: PoolReady{[]}
    case 1n+s 1n+b Con{Failed{}, _}: PoolAborted{}
    case 1n+s 1n+b Con{Known{}, tail}: signing_pool(1n+s, b, tail)
    case 1n+s 1n+b Con{Missing{id}, tail}: include(id, signing_pool(s, b, tail))

# A reference trace for a prefix of eligible requests, followed by arbitrary
# provider results. The prefix length is both the slot count and lookup budget.
def missing_prefix(ids: List<Nat>, tail: List<Lookup>) -> List<Lookup>:
  match ids:
    case Nil{}: tail
    case Con{id, rest}: Missing{id} <> missing_prefix(rest, tail)

# Count actual blocks in a contiguous query. Both RPC endpoints are inclusive.
# Pair each available block with one permit until either supply is exhausted.
def scan_count(available: Nat, limit: Nat) -> Nat:
  match available limit:
    case 0n _: 0n
    case 1n+a 0n: 0n
    case 1n+a 1n+l: 1n+scan_count(a, l)

# Each signer has at most one durable vote for a given key and ceremony.
# To have a quorum a pool must outnumber the votes for any competing pool.
def outnumbers(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n _: False{}
    case 1n+x 0n: True{}
    case 1n+x 1n+y: outnumbers(x, y)

# The current TSS key needs ceil(2 * participants / 3) distinct shares.
def can_certify(+votes: Nat, +others: Nat) -> Bool:
  outnumbers(votes, others) && Nat.is_le(Nat.add(others, others), votes)

type UnwrapStatus is Data:
  Unredeemed{}
  Pending{}
  Redeemed{}
  Revoked{}

# A pending lookup can arrive after a listener has stored a terminal status.
def apply_pending(current: UnwrapStatus) -> UnwrapStatus:
  match current:
    case Unredeemed{}: Pending{}
    case Pending{}: Pending{}
    case Redeemed{}: Redeemed{}
    case Revoked{}: Revoked{}

# Every returned block must belong to the inclusive interval before it commits.
def logs_in_range(+first: Nat, +last: Nat, heights: List<Nat>) -> Bool:
  match heights:
    case Nil{}: True{}
    case Con{+h, rest}: Nat.is_le(first, h) && Nat.is_le(h, last) && logs_in_range(first, last, rest)

def scan_checkpoint(previous: Nat, last: Nat, valid: Bool) -> Nat:
  match valid:
    case False{}: previous
    case True{}: last

# common/global_state.go & common/redeem_incident.go:
# Bridge state machine safety, emergency latch, and restart-required latch.
type BridgeState is Data:
  StateLive{}
  StateKeyGen{}
  StateHalted{}
  StateEmergency{}
  StateReSign{}

def is_protected_state(s: BridgeState) -> Bool:
  match s:
    case StateHalted{}: True{}
    case StateEmergency{}: True{}
    case _: False{}

def can_sign_transfers(s: BridgeState, redeem_halt_pending: Bool, restart_required: Bool) -> Bool:
  match s redeem_halt_pending restart_required:
    case _ True{} _: False{}
    case _ False{} True{}: False{}
    case StateEmergency{} False{} False{}: False{}
    case StateHalted{} False{} False{}: False{}
    case _ False{} False{}: True{}

def transition_allowed(current: BridgeState, requested: BridgeState, redeem_halt_pending: Bool, restart_required: Bool) -> Bool:
  match current requested redeem_halt_pending restart_required:
    case _ StateEmergency{} True{} _: True{}
    case _ _ True{} _: False{}
    case _ StateEmergency{} False{} True{}: True{}
    case _ StateHalted{} False{} True{}: True{}
    case _ _ False{} True{}: False{}
    case StateEmergency{} StateEmergency{} False{} False{}: True{}
    case StateEmergency{} StateHalted{} False{} False{}: True{}
    case StateEmergency{} _ False{} False{}: False{}
    case _ _ False{} False{}: True{}

def next_state(current: BridgeState, requested: BridgeState, redeem_halt_pending: Bool, restart_required: Bool, pending_resign: Bool) -> BridgeState:
  match current requested redeem_halt_pending restart_required pending_resign:
    case _ _ True{} _ _: StateEmergency{}
    case _ StateEmergency{} False{} True{} _: StateEmergency{}
    case _ StateHalted{} False{} True{} _: StateHalted{}
    case cur _ False{} True{} _: cur
    case StateEmergency{} StateEmergency{} False{} False{} _: StateEmergency{}
    case StateEmergency{} StateHalted{} False{} False{} _: StateHalted{}
    case StateEmergency{} _ False{} False{} _: StateEmergency{}
    case StateKeyGen{} StateLive{} False{} False{} True{}: StateReSign{}
    case StateHalted{} StateLive{} False{} False{} True{}: StateReSign{}
    case _ req False{} False{} _: req

def halt_signing_target(s: BridgeState) -> BridgeState:
  match s:
    case StateEmergency{}: StateEmergency{}
    case StateHalted{}: StateHalted{}
    case _: StateHalted{}

# rpc/evm.go & rpc/unwrap_evidence.go:
# Every configured EVM endpoint must answer and agree on the exact value.
type EndpointAnswer is Data:
  AnswerError{}
  AnswerHash{h: Nat}

type Consensus is Data:
  ConsensusNone{}
  ConsensusAgreed{h: Nat}

def keep_consensus(eq: Bool, res: Consensus) -> Consensus:
  match eq:
    case False{}: ConsensusNone{}
    case True{}: res

def agree_rest(+expected: Nat, xs: List<EndpointAnswer>) -> Consensus:
  match xs:
    case Nil{}: ConsensusAgreed{expected}
    case Con{AnswerError{}, _}: ConsensusNone{}
    case Con{AnswerHash{h}, rest}: keep_consensus(Nat.is_eq(expected, h), agree_rest(expected, rest))

def agree_all(xs: List<EndpointAnswer>) -> Consensus:
  match xs:
    case Nil{}: ConsensusNone{}
    case Con{AnswerError{}, _}: ConsensusNone{}
    case Con{AnswerHash{+h}, rest}: agree_rest(h, rest)

# tss/unwrap_agreement.go:
# Durable non-equivocating vote reservation per (key, windowSize, windowNumber).
type VoteRecord is Data:
  NoVote{}
  Voted{digest: Nat}

type VoteOutcome is Data:
  VoteAccepted{digest: Nat}
  VoteRejected{}

def vote_match(eq: Bool, digest: Nat) -> VoteOutcome:
  match eq:
    case True{}: VoteAccepted{digest}
    case False{}: VoteRejected{}

def reserve_vote(window_size: Nat, pool_nonempty: Bool, prior: VoteRecord, +candidate: Nat) -> VoteOutcome:
  match window_size pool_nonempty prior:
    case 0n _ _: VoteRejected{}
    case 1n+_ False{} _: VoteRejected{}
    case 1n+_ True{} NoVote{}: VoteAccepted{candidate}
    case 1n+_ True{} Voted{+prev}: vote_match(Nat.is_eq(prev, candidate), prev)

# network/registered_redeem.go:
# Fraud-detection decision model for EVM RegisteredRedeem events.
type RedeemAction is Data:
  RedeemRecordMatch{}
  RedeemRetry{}
  RedeemArchive{}
  RedeemHalt{}

def decide_redeem(matched: Bool, live: Bool, acknowledged: Bool, evidence_ok: Bool, settled_latest: Bool, settled_final: Bool) -> RedeemAction:
  match matched live acknowledged evidence_ok settled_latest settled_final:
    case True{} True{} _ _ _ _: RedeemRecordMatch{}
    case True{} False{} _ True{} _ _: RedeemRecordMatch{}
    case True{} False{} _ False{} _ _: RedeemRetry{}
    case False{} _ _ False{} _ _: RedeemRetry{}
    case False{} _ True{} True{} _ _: RedeemArchive{}
    case False{} True{} False{} True{} True{} _: RedeemRetry{}
    case False{} True{} False{} True{} False{} _: RedeemHalt{}
    case False{} False{} False{} True{} _ True{}: RedeemArchive{}
    case False{} False{} False{} True{} _ False{}: RedeemHalt{}
