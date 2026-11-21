# When using Bend

- Run `bend guide` before writing or changing Bend code.
- Write laws in `LAWS.bend` for properties the code must always satisfy.
- Run `bend PROOF.bend` before committing Bend changes and fix any failures.
- Parallelize independent work whenever possible, especially in performance-critical code. Preserve any ordering the laws require.
