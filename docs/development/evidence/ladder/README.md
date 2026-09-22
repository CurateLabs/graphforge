# Retained clean-ladder evidence

Ladder evidence is archived here before a run is reported. The work root
`/home/ubuntu/graphforge-ladder` on OVHC-AGENCY remains protected from tests
and cleanup tooling; the archive also preserves evidence if that root is lost.
The retained copy of a completed clean
ladder is its rung JSON, result JSON, receipts (plan, graphforge, projection),
the controller summary, and a `MANIFEST.sha256`; the archive digest is the
SHA-256 of that manifest. Raw BenchExec output is optional and is retained for
the baseline of record. For each completed ladder this is staged under
`<full 40-hex commit>/` in a PR, or kept in an out-of-tree archive whose path
and digest are recorded on the citing issues (#1478, #1387).

Run the retention step with:

```bash
make -C benchmarks retain-ladder-evidence \
  EVIDENCE_DIR=/home/ubuntu/graphforge-ladder/clean-<sha>-evidence \
  SUMMARY_LOG=/home/ubuntu/gf-clean-ladder-<sha>.log \
  DESTINATION=../docs/development/evidence/ladder/<full-sha> [INCLUDE_BENCHEXEC=1]
```

Each rung receipt is verified against the digests recorded in its
`s<scale>-result.json` before anything is copied; retention is append-only.

## Retained archives

| Commit | Contents | Archive digest (SHA-256 of `MANIFEST.sha256`) |
|---|---|---|
| `b6ffb088405150afc117ba7180c876659cec729f` | Full S18–S22 rung evidence + controller summary; the #1478 baseline of record (step 0, contains #1452, #1519, the #1526 fix) | `807609f97477e91c3cb5321261a4e9274f51573c150da88b320baf3b55c8342d` |
| `ed273d2b2558c4137a398e4980471ca3bfcac613` | Full S18–S20 rung evidence + controller summary; the #1384 closeout measurement (contains #1552) | `9baee1212766be7eca7672b11ea5b2b9d4e43accf4a08df7d68d5d4c3f40aaf1` |
| `f80f69fe02612fa789d1b4df21606d2ff79aac50` | Controller summary only (S18–S22, 2026-09-17) | `08ce651c2672a2fb7660e402e6ef3de2149db1edb70cc123702e9ae33b6fdc71` |
| `9269362fdda951756aa6887779179c5acdf214ec` | Controller summary only (S18–S20, 2026-09-17) | `04f6e8c680c6aeb02f4fd498d49b677d6ac7b924277e484afbd354dc204a9ef4` |
| `7febcb4fd855c097fc0ff69835f6e74e9a816ff4` | Controller summary only (2026-09-20) | `3fe719c5b6290e3d88c040a7cfc7276139f1325e912fe723a6d27fd967205bff` |
| `1955f17d902c7156eb6f00427e4c3f57e635e5b5` | Controller summary only (2026-09-17; the per-rung stop reference for the 2026-09-21 ladders) | `f8eb88f272d1b49bcec18a68e6e27f7c0a49195806266c4529ebdc6df3630783` |
| `ab1a713e79af875d7fc66cce18a87bd63b7681fb` | Controller summary only (S18–S22, 2026-09-21; first complete ladder on a tree containing #1452) | `7914b6a43397a488f50a72a72c0dc61ae8e0c0b14dc0c18eeb105a400b63ed27` |
| `4fbfe84cdb76242ed17f2979a559beb19a66f1b2` | Controller summary only (S18–S20, 2026-09-21; S22 stopped on #1526) | `e4c861d0fe6fa92ba1b3b81c16073b45f30c2557e78d8fc203ac656898013466` |

The `f80f69fe` summary covers its S18–S22 ladder; that tree's S24 rung — the
only S24 measurement ever taken — has no surviving primary artifact.

## Evidence lost on 2026-09-21

The ladder root was deleted and recreated at 18:48:27 UTC on 2026-09-21. Lost
beyond recovery: the rung directories `s18-s22-93c041df-evidence/`,
`clean-f80f69fe-evidence/` (including the S24 rung), `clean-1955f17d-evidence/`,
`clean-ab1a713e-evidence/`, `clean-4fbfe84c-evidence/` (including its
`s22-failure-raw/`), and `s18-403fc02a-instrumented-evidence/`, plus the frozen
ladder binaries. The controller summaries retained above are the surviving
primary artifacts for those runs. The tables copied into #1478 and #1387 are
the record for every number those directories carried; per-operation `cpu_ns`,
publication regions, and residuals for them are gone. #1387 comments
5734006616, 5734073771, and 5734143331 (the `403fc02a` instrumented evidence)
remain the record for §3.1 of the archived plan.

The `b6ffb088` evidence directory was deleted from the ladder root again at
about 01:50 UTC on 2026-09-22, after its ladder completed; the archive above
was built from the byte-identical copy made when the run was reported, whose
receipts verify against the recorded digests. This is the loss the retention
rule exists to stop.
