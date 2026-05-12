# Contributing to moves.rs

`moves.rs` is a pure-Rust port of EPA's MOVES on-road and NONROAD off-road
emissions models. The migration is tracked in
[`moves-rust-migration-plan.md`](moves-rust-migration-plan.md); each task in
the plan maps to one or more issues. This document covers the coding
conventions and CI gates the port follows. If you are picking up a task,
read the linked plan section first — it specifies which Java/Fortran
sources you are porting and what the acceptance criteria are.

## Workspace layout

The repository is a Cargo workspace. Each crate has a focused responsibility:

| Crate | Responsibility |
|-------|----------------|
| `moves-runspec` | RunSpec XML + TOML parsing and serialization (Tasks 12–13). |
| `moves-data` | Pollutant/process/source-type enums and the `DataFrameStore` (Tasks 14, 50). |
| `moves-framework` | `ExecutionRunSpec`, location iterator, MasterLoop scheduler (Tasks 15–19). |
| `moves-calculators` | The ~70 onroad emission calculators (Phase 3). |
| `moves-nonroad` | Pure-Rust port of NONROAD2008a (Phase 5). |
| `moves-cli` | Command-line entry point (`moves` binary). |
| `moves-calculator-info` | Phase 1 chain-reconstruction tool (build-time, not runtime). |
| `moves-fixture-capture`, `moves-snapshot` | Phase 0 fixture-capture + canonical snapshot format. |

Add new crates under `crates/` and register them in the root
`Cargo.toml`'s `[workspace]` `members`.

## Coding conventions

These conventions exist because they pay off later in the port; deviating from
them is fine when you have a concrete reason, but say so in the PR description
or commit message so reviewers don't reflexively bounce the change.

### Error handling

* Define crate-local error enums with [`thiserror`]. Each crate's error type
  lives in `src/error.rs` and is re-exported as `pub use error::Error` at the
  crate root, alongside `pub type Result<T> = std::result::Result<T, Error>`.
* Add `#[from]` conversions for the foreign error types you actually
  surface — don't anticipate.
* Reserve `anyhow` for binaries (`moves-cli`, dev tools). Libraries return
  concrete error enums so downstream crates can match on variants.

### Async and I/O

* Treat `tokio` as an **I/O-boundary** tool, not a general programming model.
  Wrap blocking file or database calls at the edges; keep calculator and
  framework code synchronous. The MOVES domain is CPU-bound and the runtime
  must work on WASM, so blanket `async fn` everywhere is a non-goal.
* If you find yourself sprinkling `.await` through calculator code, step back
  and push the I/O to the caller.

### DataFrames and SQL replacement

* Use [`polars`] with the **`lazy` API by default**. The MOVES Java code
  builds and discards huge MariaDB intermediates; the Rust port replaces that
  with chained lazy expressions so the optimizer can fuse projections and
  push down predicates. Materialize (`collect()`) only at the points where
  the next stage genuinely needs an in-memory frame.
* Schema definitions for shared tables live in `moves-data`. If a column
  name appears in more than one crate, hoist its `&'static str` constant to
  `moves-data` so renames are a single grep.

### Parallelism

* Use [`rayon`] for data parallelism. Build **bounded** thread pools
  (`rayon::ThreadPoolBuilder::new().num_threads(n).build()?`) at the boundary
  where you need parallelism, sized from the RunSpec or an explicit user knob,
  not the global pool. Unbounded parallelism kills throughput when the runtime
  is itself called from a parallel orchestrator.
* Polars uses rayon internally; respect its `POLARS_MAX_THREADS` knob and don't
  layer additional rayon pools on top without measuring.

### Tests

* Each crate's `tests/` directory holds integration tests that exercise the
  public API. Unit tests for private helpers live next to the code in
  `#[cfg(test)] mod tests` blocks.
* When porting Java tests, port the existing fixtures verbatim and assert on
  the same expected values. The fidelity bar is bit-for-bit identical to
  canonical MOVES wherever the migration plan says so (see Task 115).
* Tests that need a fixture file should reference fixtures under
  `tests/fixtures/<crate>/` or load them via the Phase 0 snapshot helpers in
  `moves-snapshot`.

### Formatting and lints

* `cargo fmt --all` before every commit. CI enforces it via
  `cargo fmt --all -- --check`.
* `cargo clippy --workspace --all-targets -- -D warnings`. New code should
  compile clean. If you must `#[allow(clippy::lint)]`, restrict it to the
  smallest possible scope and add a one-line comment explaining why.
* `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS=-D warnings` is part
  of CI. Doc-link breakage is a hard fail.

### Dependencies

* Prefer workspace dependencies (`<dep> = { workspace = true }`) so versions
  stay consistent. Declare new shared deps in the root `Cargo.toml`'s
  `[workspace.dependencies]` table.
* `cargo deny` runs on every push. If you add a new direct dependency,
  check its license is on the allow list in `deny.toml`; if not, add it
  with a short justification in the PR.

## CI

CI (`.github/workflows/ci.yml`) runs on every push and pull request:

* `cargo fmt --all -- --check`
* `cargo clippy --workspace --all-targets -- -D warnings`
* `cargo build --workspace --all-targets`
* `cargo test --workspace --all-targets`
* `cargo doc --workspace --no-deps` with deny-warnings
* `cargo deny check`

A separate workflow (`.github/workflows/fixture-suite-weekly.yml`) runs the
canonical-MOVES fixture-snapshot regression weekly on a self-hosted runner;
that gate is too slow for per-push and lives outside the per-push CI.

## Commits and PRs

* Prefix commit subjects with the conventional type (`feat:`, `fix:`,
  `chore:`, `docs:`, `refactor:`, `test:`, `ci:`) and reference the relevant
  task or bead ID in parentheses, e.g.
  `feat: port Task 12 RunSpec XML parser (mo-21yo)`.
* Keep commits scoped to one task where possible. Mixed-purpose commits
  make the migration trail noisier than it needs to be.
* PR descriptions should call out any numerical-fidelity caveats (see
  Task 115/116 budget) and any new dependencies.

[`thiserror`]: https://docs.rs/thiserror
[`polars`]: https://docs.rs/polars
[`rayon`]: https://docs.rs/rayon
