# Security Policy

## Supported versions

Only the latest release of moves.rs receives security fixes.

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please email security reports to the maintainers at the address listed on the
[EarthSciML GitHub organisation](https://github.com/EarthSciML). Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce (a minimal RunSpec or command if applicable)
- Your preferred contact method for follow-up

We aim to acknowledge reports within 72 hours and to issue a fix or mitigation
within 30 days of confirmation.

## Scope

moves.rs is a command-line tool and WebAssembly library. Relevant security
concerns include:

- **Malicious RunSpec/input files** — path traversal, resource exhaustion,
  or arbitrary-code execution triggered by a crafted input
- **WASM sandbox escapes** — behaviour in the browser build that violates
  browser security policies
- **Dependency vulnerabilities** — issues in upstream crates that affect
  confidentiality, integrity, or availability

Out of scope: theoretical vulnerabilities with no practical exploit path,
issues in the EPA MOVES Java application itself (report those to EPA).
