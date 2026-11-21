import Base

# network/manager.go selects ordinary unsigned wraps in Zenon RPC order.
# The RPC reports confirmations still required, rather than confirmations seen.
def wrap_is_final(remaining: Nat) -> Bool:
  match remaining:
    case 0n: True{}
    case 1n+r: False{}

# network/evm_confirm.go. Depth is checked before interpreting any receipt.
# Canonical evidence is used when a receipt is missing or contradicts the log.
type Receipt is Data:
  SuccessfulInBlock{}
  ReadError{}
  NeedsBlockCheck{}

type Canonical is Data:
  Uncertain{}
  Replacement{}
  ObservedWithLog{}
  ObservedWithoutLog{}

type Decision is Data:
  Retry{}
  Confirm{}
  RecheckReplacement{}

def confirm(mature: Bool, receipt: Receipt, block: Canonical) -> Decision:
  match mature receipt block:
    case False{} _ _: Retry{}
    case True{} SuccessfulInBlock{} _: Confirm{}
    case True{} ReadError{} _: Retry{}
    case True{} NeedsBlockCheck{} Replacement{}: RecheckReplacement{}
    case True{} NeedsBlockCheck{} ObservedWithLog{}: Confirm{}
    case True{} NeedsBlockCheck{} _: Retry{}

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
