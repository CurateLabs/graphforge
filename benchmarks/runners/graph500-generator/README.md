# Graph500 input for GraphForge lifecycle tests

**Graph500-compliant generated input; GraphForge lifecycle measurements; no
Graph500 performance/TEPS claim.** This is an unweighted input generator for
the native host scale tests, not a BFS/SSSP benchmark implementation.

For supported SCALE 1–26 it emits all `2^SCALE` node records and exactly
`16 × 2^SCALE` raw edge tuples with initiator probabilities
`A,B,C,D = 0.57,0.19,0.19,0.05`. SplitMix64 drives independent R-MAT tuple
choices; rejection of values below 16 makes reduction from 64 bits to 100
quadrant slots unbiased. Two keys from a separate `seed XOR "GRAPH500"` domain
drive one globally consistent vertex permutation. Odd multiplication and bit
reversal scramble labels without an in-memory permutation table. Tuples are
emitted in pseudorandom generation order without sorting or a post-shuffle.

The [Graph500 specification, sections 2–3](https://graph500.org/?page_id=12)
defines undirected input tuples. Self-loops and repeated endpoint pairs remain
in the raw output; each tuple receives a distinct edge UUID. The GraphForge
adapter stores one `EDGE` record per tuple and uses the emitted first and second
endpoints as source and target. Its directed Cypher queries measure GraphForge
behavior on that orientation; they are not Graph500 traversal results. No
deduplication, loop removal, reciprocal-edge expansion, or replenishment occurs.
Raw and stored counts are consequently equal; at SCALE26 both are
1,073,741,824. These are edge records, not unique undirected pairs.

Endpoints use `u64` and Parquet `FixedSizeBinary(16)` UUIDs; the identifier
mapping preserves at least 48 index bits. SCALE26 is the supported generation
limit, independent of the identifier width. Output batches contain at most
65,536 rows and do not restart either the quadrant stream or permutation keys.

## Reference provenance and verification

Only the vertex scramble is ported from Graph500. Its copyright is
**2009–2010 The Trustees of Indiana University**, authors Jeremiah Willcock and
Andrew Lumsdaine, under the accompanying [Boost Software License 1.0](LICENSE_1_0.txt).
The pinned source is
[`graph500-3.0.0`, commit `9dbb76c7db4d00dc12fc44d02ba8bd2532236292`](https://github.com/graph500/graph500/blob/9dbb76c7db4d00dc12fc44d02ba8bd2532236292/generator/graph_generator.c).
The reference's MRG stream and clip-and-flip implementation are not used;
the complete GraphForge tuple sequence is not claimed to equal reference bytes.

`tests/reference/graph500_scramble.c` retains the pinned C helper independently
of the Rust port. The Python verifier compiles it and checks frozen scramble
vectors, including 48-bit labels, plus complete-generator vectors obtained
from independent Python integer arithmetic and that C helper. The complete
SCALE1/seed1 tuple list and prefixes at SCALE6/seed7 and SCALE26/seed `u64::MAX`
are frozen. Rust tests consume those vectors and compare decoded Parquet output
across different write batch sizes.

```bash
python3 benchmarks/runners/graph500-generator/tests/reference/verify_vectors.py
cargo test --locked --manifest-path benchmarks/Cargo.toml -p graphforge-benchmark-graph500-generator
```

All behavior-bearing helpers remain in `src/main.rs`, which current profiles
and evidence hash. License text and reference-only fixtures do not alter
generated data. When the generator changes, refresh active profile identities
with the repository's generator identity tool; preserve historical evidence.
