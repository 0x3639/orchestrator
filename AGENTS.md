# When using Bend

- Run `bend guide` before writing or changing Bend code.
- Write laws in `LAWS.bend` for properties the code must always satisfy.
- Derive expected behavior from the bridge contract or a documented requirement. Challenge the Go implementation at its real entry points with boundary cases and sequences of failures.
- When a comparison fails, keep the counterexample as a regression test. Change a law only when independent evidence shows that its requirement was wrong or has changed.
- Run `bend PROOF.bend` before committing Bend changes. Report Go comparison failures with the input and observed behavior.
- Parallelize independent work whenever possible and preserve any ordering the laws require. For a verification-only change, record proposed runtime optimizations in a follow-up issue.
