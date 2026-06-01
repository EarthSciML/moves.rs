# moves.rs

A pure-Rust port of EPA's [MOVES](https://www.epa.gov/moves) on-road and NONROAD off-road emissions model.

> **Not for regulatory use.** This port targets the research and policy community. See the [regulatory caveat](#regulatory-caveat).

## What it is

MOVES (Motor Vehicle Emission Simulator) is the U.S. EPA's official model for estimating emissions from cars, trucks, and non-road equipment. The canonical implementation requires MariaDB, a JVM, and in some configurations a multi-machine worker cluster.

`moves.rs` is a from-scratch Rust port that:

- Ships as a **single static binary** — no MariaDB, no JDK, no database server
- Reads the same RunSpec XML files as canonical MOVES (plus a new TOML format)
- Runs both the onroad and [NONROAD](https://www.epa.gov/moves/nonroad-model-nonroad-engines-equipment-and-vehicles) model paths
- Produces output as **Parquet** rather than MariaDB tables
- Includes a **WebAssembly build** for browser-based use

## Status — v0.1.0

The port covers all ~70 onroad calculators, the full NONROAD model (a
pure-Rust rewrite of the 29k-line Fortran NONROAD2008a), and all four control
strategies. The characterization suite (37 fixtures) completes without error,
including emission output.

`moves run` plans the full calculator graph, parses your RunSpec correctly,
feeds the default-database Parquet data into the calculator context, and
produces emission rows. See [what's not yet supported](docs/porting-guide.md#what-is-not-yet-supported)
for any remaining limitations.

## Quick start

```bash
# Install from source (Rust 1.78+)
git clone https://github.com/EarthSciML/moves.rs
cd moves.rs
cargo build --release --locked
./target/release/moves --version

# Run the included sample RunSpec
./target/release/moves run \
 --runspec characterization/fixtures/sample-runspec.xml \
 --output /tmp/moves-out
```

Pre-built binaries for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64) are available on the [Releases page](https://github.com/EarthSciML/moves.rs/releases).

See [docs/user-guide.md](docs/user-guide.md) for the full installation guide,
RunSpec format reference, and output schema.

## Browser demo

The port compiles to WebAssembly and runs in modern browsers with no server-side computation.

**Hosted demo:** [https://earthsciml.github.io/moves.rs/demo/](https://earthsciml.github.io/moves.rs/demo/)
(published to GitHub Pages automatically on each push to `main` that passes CI, alongside the API docs)

To run the demo locally instead:

```bash
# Build the WASM package
wasm-pack build --target web crates/moves-wasm

# Serve the demo
python3 -m http.server 8080 --directory crates/moves-wasm
# Open http://localhost:8080/demo/
```

See [crates/moves-wasm/demo/README.md](crates/moves-wasm/demo/README.md) and
[docs/wasm-embedding.md](docs/wasm-embedding.md).

## Documentation

| Document | Contents |
|----------|----------|
| [docs/user-guide.md](docs/user-guide.md) | Installation, getting started, RunSpec reference |
| [docs/porting-guide.md](docs/porting-guide.md) | Migrating from canonical MOVES |
| [docs/known-divergences.md](docs/known-divergences.md) | Characterization results and divergence register |
| [docs/benchmark-report.md](docs/benchmark-report.md) | Performance measurements and projections |
| [docs/developer-guide.md](docs/developer-guide.md) | Architecture, contributing a calculator |
| [docs/control-strategies.md](docs/control-strategies.md) | AVFT, Rate-of-Progress, OnRoadRetrofit, LEV |
| [docs/wasm-embedding.md](docs/wasm-embedding.md) | Embedding the WASM module |
| [docs/upstream-tracking.md](docs/upstream-tracking.md) | Incorporating annual EPA MOVES updates |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Code conventions, CI gates, how to contribute |
| [CHANGELOG.md](CHANGELOG.md) | Release history |

## Community and feedback

**Bug reports and feature requests:** open an issue on
[GitHub Issues](https://github.com/EarthSciML/moves.rs/issues). Issue templates
are available for bug reports, feature requests, and numerical-divergence
reports. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide.

**Security vulnerabilities:** see [SECURITY.md](SECURITY.md) — do not open a
public issue.

**Questions and discussion:** use
[GitHub Discussions](https://github.com/EarthSciML/moves.rs/discussions) for
open-ended questions about RunSpec configuration, output interpretation, or
research use cases.

## Regulatory caveat

> This port is **not approved for regulatory use**. Do not use it for SIP
> submissions, transportation conformity determinations, NEPA analyses,
> NAAQS-related filings, or any other regulatory purpose.

The port is intended for research and policy analysis. Regulatory validity
requires formal EPA approval and a passing EPA validation suite — neither of
which has been pursued for this port. Researchers requiring regulatory-grade
output must continue using the official [MOVES Java application](https://www.epa.gov/moves).

## License

Licensed under the [MIT License](LICENSE).

MOVES is developed by the U.S. Environmental Protection Agency. This port is
not affiliated with, endorsed by, or approved by EPA.
