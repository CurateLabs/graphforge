//! In-process Divan benchmark over the openCypher TCK scenarios (#1653).
//!
//! Parses the same ephemeral normalized corpus as the Cucumber correctness run
//! (`tests/bdd/main.rs`) and times each scenario, executed through the same
//! registered step functions, pooled fixture and clear-on-lease semantics. See
//! `runner.rs` for the timed region and the fail-closed verdict check.
//!
//! ```bash
//! cargo bench -p graphforge-api --bench tck_scenarios              # measure
//! cargo bench -p graphforge-api --bench tck_scenarios -- --test    # test mode
//! ```
//!
//! Divan does the measuring. Machine-readable per-scenario output is CodSpeed's
//! walltime `raw_results`, written only when `CODSPEED_ENV` is set; see
//! `docs/development/benchmarking.md`. Test mode runs every scenario once and
//! emits no timing. `TCK_ONLY=<substr>` restricts the corpus as it does for the
//! Cucumber run.

#[cfg(feature = "search")]
#[path = "../../tests/bdd/api_steps.rs"]
mod api_steps;
#[path = "../../tests/bdd/corpus.rs"]
mod corpus;
#[path = "../../tests/bdd/fixture.rs"]
mod fixture;
mod runner;
#[path = "../../tests/bdd/tck_steps.rs"]
mod tck_steps;
#[path = "../../tests/bdd/world.rs"]
mod world;

use world::GraphForgeWorld;

fn main() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root must exist");
    let (normalized, cases) = runner::load_normalized(&workspace_root.join("tests/tck/features"));
    eprintln!(
        "TCK scenario benchmark: {} scenarios, fixture profile pooled-isolated-serial-v1, concurrency {}",
        cases.len(),
        fixture::TCK_CONCURRENCY
    );
    runner::install_corpus(cases);

    let fixture_guard = fixture::activate();
    divan::main();
    runner::assert_fixture_profile();
    drop(fixture_guard);
    drop(normalized);
}
