# krafka task runner.
#
# This file is the single source of truth for what "the checks" are.
# `.github/workflows/ci.yml` calls these recipes rather than repeating the
# commands, so a check cannot pass locally and fail in CI because the two
# drifted apart. If you change a feature string here, CI changes with it.
#
#   just            list every recipe
#   just ci         everything CI runs, except the Docker-backed suites
#   just pre-commit the fast subset worth running before every commit
#
# Requires: just (https://just.systems). Optional tools are detected at run
# time and skipped with an explanation rather than failing the run.

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# ── Feature sets ────────────────────────────────────────────────────────────
#
# `ring` and `rustls-aws-lc-rs` are additive: with both enabled aws-lc-rs wins
# (see `auth::tls::resolve_crypto_provider`), so `--all-features` is a valid
# configuration and needs no hand-maintained exclusion list.

# Everything except the aws-lc-rs backend, so the default `ring` code paths are
# the ones that actually execute. `cfg(not(feature = "rustls-aws-lc-rs"))` arms
# exist in `auth/tls.rs` and `schema_registry/http.rs` and run nowhere else.
ring_features := "compression-all,aws-msk,oauth-oidc,native-tls-roots,unstable-protocol,telemetry,socks5,ring"

# Portable subset for macOS and Windows: pure-Rust `ring` avoids needing a C
# toolchain and NASM. `test-broker` is deliberately included — it binds real
# TCP listeners and drives real clients over loopback, which is the behaviour
# most likely to differ between platforms.
cross_platform_features := "compression,oauth-oidc,unstable-protocol,telemetry,socks5,test-broker,ring"

# Minimum supported Rust version, mirroring `rust-version` in Cargo.toml.
msrv := "1.88"

# Default recipe: show what is available.
default:
    @just --list --unsorted

# ── The umbrella recipes ────────────────────────────────────────────────────

# Everything CI runs, except the Docker-backed integration suites.
#
# Ordered cheapest-first so a formatting slip fails in seconds rather than
# after a full test run.
[doc("Everything CI runs (no Docker suites)")]
ci: fmt-check clippy check protocol-parity protocol-reachability secret-debug test-reachability config-reachability version-check site-check docs-test test-ring test minimal-features doc
    @echo ""
    @echo "✓ ci passed — Docker suites not included, run 'just integration' for those"

# Everything, including the Docker-backed integration suites. This is what a
# release should be gated on. Mirrors CI's Docker jobs: the plain suite (CI
# additionally runs it across Kafka 3.9 → 4.3 — `just integration-matrix`
# locally), the SASL suite, and the Redpanda suite.
[doc("ci + supply chain + Docker integration suites")]
ci-full: ci deny integration integration-sasl integration-redpanda
    @echo ""
    @echo "✓ ci-full passed"

# The fast subset worth running before every commit.
pre-commit: fmt-check clippy check
    @echo ""
    @echo "✓ pre-commit passed"

# Install this as a git pre-commit hook.
install-hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    hook=.git/hooks/pre-commit
    printf '#!/usr/bin/env bash\nexec just pre-commit\n' > "$hook"
    chmod +x "$hook"
    echo "✓ installed $hook -> just pre-commit"

# ── Individual checks (each mirrors one CI job) ─────────────────────────────

# Formatting, check-only.
fmt-check:
    cargo fmt --all -- --check

# Rewrite files to satisfy the formatter.
fmt:
    cargo fmt --all

# Lint every target and feature. Warnings are errors, matching CI.
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Type-check every target and feature.
check:
    cargo check --all-targets --all-features

# Full test suite with every feature enabled.
test:
    cargo test --all-features

# Test with the *default* crypto backend, which is what most downstream users
# compile. Type-checking is not enough here: the ring-only arms have to run.
[doc("Test with the default `ring` backend only")]
test-ring:
    cargo test --all-targets --no-default-features --features "{{ring_features}}"

# The portable feature subset used on macOS and Windows in CI.
test-cross-platform:
    cargo test --no-default-features --features "{{cross_platform_features}}"

# Guard the minimum viable configuration, and pin feature additivity.
#
# A missing crypto backend must fail with the `compile_error!` in lib.rs, not
# deep inside rustls; and enabling both backends must build, because Cargo
# features are additive and a dependency may well enable the other one.
[doc("Minimum viable config, and both backends together")]
minimal-features:
    cargo check --no-default-features --features "ring"
    cargo check --no-default-features --features "ring,rustls-aws-lc-rs"

# Check the API version table against Apache Kafka's own message schemas.
#
# Reads a vendored snapshot, so it needs no network and cannot flake. It catches
# the drift class a reviewer found by hand: four APIs pinned below their stable
# Kafka ceiling, and a `Fetch` v17/v18 gate that existed only in prose.
[doc("API version table must match the vendored Kafka schema snapshot")]
protocol-parity:
    python3 xtask/protocol_parity.py

# No credential-bearing type may derive Debug.
#
# `Debug` is the quiet way secrets reach a log aggregator: a `tracing` field, an
# error context or a panic message that formats the enclosing struct is enough.
# Two instances shipped before this check existed — the OIDC client secret and
# the raw SASL payload, which for PLAIN is the password in cleartext.
[doc("No credential-bearing type may derive Debug")]
secret-debug:
    python3 xtask/secret_debug.py

# No test may assert over its own literals.
#
# A negative control is the only proof a test works. Running one against the
# share-group model produced a green suite, and the same suspicion applied to
# the rest of the suite found a test that re-implemented the condition it
# claimed to check — so deleting the guard it covered (a dry run that silently
# applies a data-lossy feature downgrade) left it green.
[doc("No test may assert over its own literals")]
test-reachability:
    python3 xtask/test_reachability.py

# Every configuration field is settable from its builder and readable back.
#
# This crate has shipped the same defect three times: a field declared,
# documented, wired to the wire protocol — and reachable from no public
# builder. `tests/builder_surface.rs` proves named methods exist; only a
# field-driven check can prove nothing was forgotten.
[doc("Every config field is settable and readable")]
config-reachability:
    python3 xtask/config_reachability.py

# Every decoded response field is read by client code.
#
# The mirror image of `config-reachability`, and the shape of this project's
# two most severe defects: `last_stable_offset` and KIP-1222's
# `acquisition_lock_timeout_ms` were both decoded correctly, round-tripped in
# the codec's own tests, and read by nobody — so the information the broker
# sent never reached the application.
[doc("Every decoded response field is read")]
protocol-reachability:
    python3 xtask/protocol_reachability.py

# Every place that names krafka's own version must agree with Cargo.toml.
#
# A bump is a search-and-replace, and search-and-replace is blind to anywhere
# already stale: bumping 0.14 -> 0.15 could not find `fuzz/Cargo.lock`, which
# had been pinned at 0.12.0 for two releases. This asserts the invariant
# instead of searching for a value.
[doc("krafka's version is consistent everywhere it appears")]
version-check:
    python3 xtask/version_check.py

# Structural invariants for the documentation site.
#
# Zola fails the build on an unparseable page or a broken internal link. It
# cannot see a duplicate nav weight or a page no index links to — both are
# valid sites — and the Jekyll setup this replaced had four of the first and
# one of the second (a 711-line schema-registry guide reachable from nothing).
[doc("Documentation site structure is sound")]
site-check:
    python3 xtask/site_check.py
    python3 xtask/doc_api.py

# Mutation-test the invariant-dense modules.
#
# Not part of `ci`: 8 200 mutants across the crate is roughly 45 CPU-hours, and
# the timing-based fake-broker tests turn many mutants into timeouts rather than
# failures. Scoped to pure, fast, high-consequence code — sequence arithmetic,
# the in-flight barrier, varint codecs — where a surviving mutant means a real
# assertion is missing.
#
# It has already earned its keep: four mutants of `is_newer_sequence` survived,
# because every existing test used values where subtracting, adding and
# dividing all landed on the same side of the threshold.
[doc("Mutation-test the invariant-dense modules")]
mutants *ARGS:
    cargo mutants \
        --file src/barrier.rs \
        --file src/producer/idempotent.rs \
        --file src/util.rs \
        --file src/consumer/fetch_session.rs \
        -j 4 --timeout 120 {{ARGS}} -- --all-features --lib

# Compile the guide snippets marked ```rust,compile.
#
# `doc_api.py` checks that names resolve; it cannot check that a call has the
# right shape, because a plausible snippet reaches only for names that exist.
# Three fabricated APIs passed it, and the documented way to implement the
# crate's own `DeadLetterQueue` trait shipped broken for two releases. This
# compiles the snippets instead.
[doc("Documentation snippets compile")]
docs-test:
    python3 xtask/docs_test.py

# Build the documentation site into site/public.
[doc("Build the documentation site")]
site-build:
    cd site && zola build

# Serve the documentation site with live reload on http://127.0.0.1:1111.
[doc("Serve the documentation site locally")]
site-serve:
    cd site && zola serve

# Re-fetch the vendored Kafka schema snapshot. Run deliberately, review the
# diff, then run `just protocol-parity` to see what krafka must do about it.
#
#   just refresh-protocol-snapshot 4.3
[doc("Refresh the vendored Kafka protocol snapshot (needs network)")]
refresh-protocol-snapshot ref="4.3":
    python3 xtask/protocol_parity.py --refresh --ref {{ref}}

# Build the docs with warnings denied, matching CI.
doc:
    RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps --all-features
    # Again over the private items. `broken_intra_doc_links` is allow-by-default
    # for anything rustdoc does not render, so the public pass alone let a link
    # to a deleted `ProducerConfigBuilder` sit in the transport docs for several
    # releases. This crate leans on its internal comments — the second pass is
    # what keeps the links in them real.
    # `redundant_explicit_links` is allowed here only: documenting private items
    # makes more paths resolvable, so a link written with an explicit target for
    # the public reader's benefit becomes "redundant" in this pass alone. The
    # correctness lints stay denied.
    RUSTDOCFLAGS="-Dwarnings -A rustdoc::redundant_explicit_links" cargo doc --no-deps --all-features --document-private-items

# Open the docs in a browser.
doc-open:
    cargo doc --no-deps --all-features --open

# Supply-chain audit: advisories, license policy, banned crates, sources.
deny:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v cargo-deny >/dev/null 2>&1; then
        echo "⊘ cargo-deny not installed — skipping."
        echo "  Install with: cargo install --locked cargo-deny"
        exit 0
    fi
    cargo deny check advisories bans licenses sources

# Integration tests against a real Kafka in Docker.
#
# `--test-threads=1` is required: the suite starts containers and shares
# cluster state between tests.
[doc("Integration tests against a real Kafka in Docker")]
integration:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker info >/dev/null 2>&1; then
        echo "✗ Docker is not available; integration tests need it." >&2
        exit 1
    fi
    cargo test --test integration_tests -- --ignored --test-threads=1

# SASL integration tests against a real Kafka in Docker.
integration-sasl:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker info >/dev/null 2>&1; then
        echo "✗ Docker is not available; integration tests need it." >&2
        exit 1
    fi
    cargo test --test sasl_integration_tests -- --ignored --test-threads=1

# Integration tests against a real Redpanda in Docker.
#
# Redpanda speaks the Kafka wire protocol; krafka negotiates every API version,
# so this suite pins the compatibility that negotiation is supposed to buy —
# including the KIP-890 TV1 fallback (Redpanda has no server-side TV2).
#
#   REDPANDA_VERSION=v25.1.1 just integration-redpanda   # pin a tag
[doc("Integration tests against a real Redpanda in Docker")]
integration-redpanda:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker info >/dev/null 2>&1; then
        echo "✗ Docker is not available; integration tests need it." >&2
        exit 1
    fi
    cargo test --test redpanda_integration_tests -- --ignored --test-threads=1

# Run the Docker integration suite against every supported Kafka minor.
#
# The harness reads `KAFKA_VERSION` (image tag), so a version matrix is one
# loop. 3.9 is the supported floor; 4.3 is the protocol-parity target.
#
#   just integration-matrix                    # the default matrix
#   just integration-matrix "4.2.0 4.3.0"      # a subset
[doc("Integration tests across Kafka 3.9 → 4.3")]
integration-matrix versions="3.9.0 4.0.0 4.1.0 4.2.0 4.3.0":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker info >/dev/null 2>&1; then
        echo "✗ Docker is not available; integration tests need it." >&2
        exit 1
    fi
    for v in {{versions}}; do
        echo "▶ Kafka $v"
        KAFKA_VERSION="$v" cargo test --test integration_tests -- --ignored --test-threads=1
    done
    echo "✓ integration-matrix passed for: {{versions}}"

# Check that the crate still builds on its declared MSRV.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! rustup run "{{msrv}}" cargo --version >/dev/null 2>&1; then
        echo "⊘ Rust {{msrv}} not installed — skipping."
        echo "  Install with: rustup toolchain install {{msrv}}"
        exit 0
    fi
    rustup run "{{msrv}}" cargo check

# ── Development helpers ─────────────────────────────────────────────────────

# Run one test by name across every feature, with output shown.
#
#   just t corrupt_record
[doc("Run one test by name, with output shown")]
t pattern:
    cargo test --all-features {{pattern}} -- --nocapture

# Watch the tree and re-run the fast checks on every change.
watch:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v cargo-watch >/dev/null 2>&1; then
        echo "✗ cargo-watch not installed. Install with: cargo install cargo-watch" >&2
        exit 1
    fi
    cargo watch -x "check --all-features" -x "test --all-features --lib"

# Run the criterion benchmarks.
bench:
    cargo bench --all-features

# Run one fuzz target. Requires a nightly toolchain and cargo-fuzz.
#
#   just fuzz fuzz_record_batch
[doc("Run one fuzz target for N seconds (default 60)")]
fuzz target time="60":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v cargo-fuzz >/dev/null 2>&1; then
        echo "✗ cargo-fuzz not installed. Install with: cargo install cargo-fuzz" >&2
        exit 1
    fi
    cargo +nightly fuzz run {{target}} -- -max_total_time={{time}}

# List the available fuzz targets.
fuzz-list:
    @ls fuzz/fuzz_targets/*.rs | xargs -n1 basename | sed 's/\.rs$//'

# Remove build artifacts.
clean:
    cargo clean

# ── Release ─────────────────────────────────────────────────────────────────

# Everything a release should be gated on, plus packaging checks.
release-check: ci-full
    #!/usr/bin/env bash
    set -euo pipefail
    echo "▶ release build"
    cargo build --release --all-features
    echo "▶ examples"
    cargo build --release --examples --all-features
    echo "▶ benches"
    cargo build --release --benches --all-features
    echo "▶ packaging"
    cargo publish --dry-run --allow-dirty
    echo ""
    echo "✓ release-check passed for v$(just version)"

# Print the crate version from Cargo.toml.
version:
    @grep -m1 '^version' Cargo.toml | cut -d'"' -f2

# Publish to crates.io. Runs the full release gate first.
publish: release-check
    cargo publish
