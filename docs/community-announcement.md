# moves.rs v0.1.0 — Community Announcement

This document contains the announcement text for the v0.1.0 release.
Adapt and post to the venues listed at the end.

---

## Announcement text

**Subject: moves.rs v0.1.0 — pure-Rust port of EPA MOVES, first public release**

We are releasing v0.1.0 of `moves.rs`, a pure-Rust port of EPA's MOVES
on-road and NONROAD off-road emissions model.

**Repository:** https://github.com/EarthSciML/moves.rs 
**Release:** https://github.com/EarthSciML/moves.rs/releases/tag/v0.1.0

### What it is

MOVES is the U.S. EPA model for estimating emissions from on-road vehicles
and non-road equipment. The canonical implementation requires MariaDB, a JVM,
and a multi-step installation procedure.

`moves.rs` is a from-scratch Rust rewrite that ships as a **single static
binary** with no external dependencies. It reads the same RunSpec XML files
as canonical MOVES, runs both the onroad and NONROAD model paths, and writes
output as Parquet rather than MariaDB tables.

### What's in v0.1.0

- All ~70 onroad emission calculators (running exhaust, start, extended idle,
 brakewear, tirewear, evaporative emissions, crankcase, APU, speciation chains)
- Full NONROAD port: a pure-Rust rewrite of the 29k-line Fortran NONROAD2008a
 model, covering all equipment categories
- All four control strategies (AVFT, Rate-of-Progress, OnRoadRetrofit, LEV)
- Both XML and TOML RunSpec formats
- County Database (CDB) and Project Database (PDB) importers
- WebAssembly build: the full onroad + NONROAD simulation runs in modern browsers

### Current limitation: data plane

**The default-database data-plane wiring is not yet complete.** `moves run` correctly
parses RunSpecs, plans the full calculator graph, and creates output file
structure — but calculators return empty output because the default-database
Parquet feed into the calculator context is not wired yet.

Use v0.1.0 to:
- Validate that your RunSpec parses correctly
- Inspect the planned calculator graph
- Exercise control-strategy configuration
- Experiment with the WebAssembly browser build

Emission numbers will arrive with the default-database data-plane milestone.

### Performance outlook

Once the data plane is wired, the port is expected to deliver 10–50× wall-time
improvement over canonical MOVES on County/Project scale runs, primarily from
eliminating MariaDB I/O and filesystem-mediated bundle handoff. At framework
level (no data), the port plans 44 calculator modules and writes output in under
5 ms at 12 MiB RSS.

### Regulatory caveat

This port is **not approved for regulatory use** and must not be used for SIP
submissions, conformity determinations, NEPA analyses, or any other regulatory
purpose. The port targets the research and policy community.

### Contributing

Contributions are welcome. See [CONTRIBUTING.md](../CONTRIBUTING.md) for the
coding conventions, CI gates, and how to pick up a task. Each entry in
`moves-rust-md` is a self-contained unit of work; the next
major milestone, default-database data-plane wiring, is ready for contributors.

### Browser demo

The WASM build can be run locally:

```bash
cargo install wasm-pack
wasm-pack build --target web crates/moves-wasm
python3 -m http.server 8080 --directory crates/moves-wasm
# Open http://localhost:8080/demo/
```

---

## Suggested venues

### Email lists and forums

- **MOVES Users Group** (EPA-hosted mailing list for MOVES users and
 developers): appropriate for a brief technical announcement noting the
 availability of the port, the regulatory caveat, and a link to the repo.

- **Transportation Research Part D / emissions modeling researchers**: reach
 via ResearchGate, direct email to active MOVES-paper authors, or posting
 to relevant LinkedIn groups (e.g., "Air Quality Modeling and Analysis").

- **State and local air quality agency contacts**: SIP modelers at state
 agencies who use MOVES for inventory work may be interested in a research
 tool that runs faster and produces Parquet output. Emphasize the regulatory
 caveat prominently.

### Academic / open-source channels

- **crates.io** blog / `r/rust` subreddit: the technical achievement of porting
 a 29k-line Fortran model + 70-calculator Java/SQL/Go stack to pure Rust is
 of general interest to the Rust community.

- **OpenStreetMap / OpenTransportData communities**: researchers doing
 emissions-by-link analyses on OSM road networks may benefit from a
 faster, scriptable MOVES alternative.

- **ORCID / Zenodo**: publish a citable DOI for the repository so academic
 users can reference the specific version in papers.

### Conference venues

The following venues have active MOVES-related sessions:

- Transportation Research Board (TRB) Annual Meeting — January
- American Association for Aerosol Research (AAAR)
- EPA's National Air Quality Conference
- AGUFM (American Geophysical Union) — Fall Meeting, atmospheric science sessions

---

## Announcement checklist

- [ ] Post to MOVES Users Group email list
- [ ] Post to relevant research community channels
- [ ] Publish GitHub Release at https://github.com/EarthSciML/moves.rs/releases
- [ ] Add citable DOI via Zenodo (archive the tag)
- [ ] Update project website / landing page if applicable
