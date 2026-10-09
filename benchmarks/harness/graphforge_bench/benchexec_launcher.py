"""Run the BenchExec CLI under the `fork` multiprocessing start method.

BenchExec loads its tool-info module in a `multiprocessing.Pool` worker that it
moves into a container (`benchexec.containerized_tool`). That worker unshares a PID
namespace and forks, so the process the pool knows is only a waiting parent. When
BenchExec exits it terminates the pool with SIGTERM, which kills that parent and
leaves the real worker orphaned, reparented to the nearest subreaper.

Since Python 3.14 the default start method is `forkserver`. A forkserver worker
inherits the write end of the resource tracker's pipe, so the orphan keeps the
tracker alive, and BenchExec's exit handler waits for the tracker forever: the run
has finished, its results are written, and `benchexec` never exits (#1914). The
`fork` start method, BenchExec's default before 3.14, starts no tracker, so
there is nothing for an orphan to hold.
"""

import multiprocessing
import sys


def main() -> None:
    """Run `benchexec`'s own entry point (it handles signals and calls `sys.exit`)."""
    multiprocessing.set_start_method("fork", force=True)
    from benchexec.benchexec import main as benchexec_main

    sys.argv[0] = "benchexec"
    benchexec_main()


if __name__ == "__main__":
    main()
