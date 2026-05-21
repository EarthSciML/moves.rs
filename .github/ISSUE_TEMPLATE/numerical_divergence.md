---
name: Numerical divergence
about: Report output values that differ from canonical MOVES beyond documented tolerances
labels: numerical-fidelity
---

## Summary

<!-- One-line description: which pollutant, source type, or model
     path shows the divergence, and approximately how large. -->

## Reproduction

```
# RunSpec that demonstrates the divergence
```

**moves.rs version:** (run `moves --version`)  
**Canonical MOVES version:**  
**Platform for canonical MOVES run:**

## Divergence details

<!-- Paste the relevant rows from moves.rs output and canonical MOVES
     output side-by-side, or describe the direction and magnitude. -->

| Metric | moves.rs | Canonical MOVES | Difference |
|--------|----------|-----------------|------------|
|        |          |                 |            |

## Expected tolerance

<!-- Does this fall within or outside the tolerance documented in
     [docs/known-divergences.md](../docs/known-divergences.md)?
     If outside, which table or section covers this case? -->

## Additional context

<!-- Platform details, any preprocessing of the RunSpec, etc. -->
