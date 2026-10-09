+++
title = "Project and Support"
description = "Who maintains krafka, which versions are supported, how to report a vulnerability, and the checks every change passes."
weight = 150

[extra]
slug_id = "governance"
+++

## Maintainer

krafka is maintained by Frank Hübner
([@hupe1980](https://github.com/hupe1980)), who reviews and merges every
change and cuts every release. Outside contributions are pull requests and go
through the same checks and review.

## Support

- **Supported versions.** krafka is pre-1.0. Only the latest minor release
  receives fixes, security fixes included; a minor release may carry breaking
  changes, each listed under `Breaking` in
  [CHANGELOG.md](https://github.com/hupe1980/krafka/blob/main/CHANGELOG.md),
  and [Upgrading to 0.27](@/docs/upgrading.md) maps the current one.
- **Release cadence.** Releases ship when a change is ready, not on a
  schedule; dates are in CHANGELOG.md.
- **Rust and Kafka.** The minimum supported Rust version is 1.95, checked by
  `just msrv`. Supported brokers are Apache Kafka 3.9 and later; the Docker
  suite runs against each supported minor and a pinned Redpanda release.
- **Questions and bugs.** Open an issue on
  [GitHub](https://github.com/hupe1980/krafka/issues). There is no paid
  support and no response-time promise for issues.

## Security

Report a vulnerability privately through GitHub's private vulnerability
reporting, never in a public issue. The process, response times and how to
verify a published crate against its build-provenance attestation are in
[SECURITY.md](https://github.com/hupe1980/krafka/blob/main/SECURITY.md).

## How a change is checked

The `justfile` defines every check, and CI runs the same recipes. A pull
request merges only when the single
required check, which depends on every CI job, passes. `just ci` runs:

| Recipe | What fails it |
|---|---|
| `fmt-check`, `clippy`, `check`, `doc` | formatting; any lint, with `unsafe_code`, `panic`, `unwrap` and `expect` denied; a broken doc link |
| `test`, `test-ring`, `test-aws-lc`, `minimal-features` | the test suites under each TLS backend and the smallest feature set |
| `sim`, `cancel-safety` | a seeded fault workload breaking an invariant; a data-path method without a cancel-safety section |
| `protocol-parity`, `protocol-reachability`, `fuzz-coverage` | the API version table disagreeing with Kafka's schemas; a version nothing negotiates; a version with no fuzz path |
| `claims-check`, `docs-test`, `site-check` | a capability claim not generated from its registry; a documentation sample that does not compile; a guide naming an API the crate does not have |
| `config-reachability`, `test-reachability` | a setting no builder reaches; a public API no test names |
| `no-c`, `no-otel`, `advisory-floors`, `secret-debug` | C beyond `ring` in the default build; an OpenTelemetry crate in the graph; a dependency floor with a known advisory; a credential type deriving `Debug` |
| `version-check`, `ci-job-parity`, `workflow-lint` | versions out of step; a recipe with no CI job; an unsafe workflow pattern |

Beyond `just ci`, CI runs the Docker integration suites against several Kafka
versions and Redpanda, `just deny` for licences and advisories, fuzzing on
each pull request and nightly, and mutation testing. Releases are published
only from the release workflow, with an attested crate and SBOM.

## AI-assisted development

krafka is developed with AI assistance. Every change passes the gates above
and the maintainer's review before it merges.
