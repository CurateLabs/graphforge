# Historical F1/perf tooling preservation (#1539)

Imported from `measure/1387-f1-f2-fio-perf` through `e6ddeb84`, cited in
[#1387](https://github.com/CurateLabs/graphforge/issues/1387#issuecomment-5737194644).
These scripts preserve the measurement method. They establish neither a CPU
budget nor a serial fraction, and do not establish performance-floor acceptance.
Those historical conclusions remain withdrawn.

Run only when an operator explicitly requests a measurement on a suitable host.
Provide executable GraphForge and generator paths, a **new** output-directory
path, and `QUIET_HELPER`, an executable reporting `QUIET` or `BUSY` as its first
line with exit status zero. Any other response or nonzero status aborts. For
anchors, also set `SORT_INPUT` to an existing input file. The event lists are
host-specific; unsupported counters are unavailable evidence, not zeros.

The runners use sudo for perf, watchdog configuration and page-cache dropping.
They record and restore the original watchdog value. Output and workspace live
in an exclusively created directory; failures retain their artifacts. Each
validation must exit successfully and emit a validated JSON receipt. Offline
regressions use stub commands and never tune the host or run workloads.
