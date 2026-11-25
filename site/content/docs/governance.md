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
  [CHANGELOG.md](https://github.com/hupe1980/krafka/blob/main/CHANGELOG.md).
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

Run `just ci` for local checks. CI also exercises supported Kafka versions and
Redpanda, runs the fuzz targets, and checks dependency licences and advisories.
The [justfile](https://github.com/hupe1980/krafka/blob/main/justfile) lists the recipes.

## AI-assisted development

krafka is developed with AI assistance. Every change passes the gates above
and the maintainer's review before it merges.
