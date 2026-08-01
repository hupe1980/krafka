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
ring_features := "compression-all,aws-msk,schema-registry,aws-glue-schema-registry,native-tls-roots,unstable-protocol,telemetry,socks5,ring"

# Portable subset for macOS and Windows: pure-Rust `ring` avoids needing a C
# toolchain and NASM. `test-broker` is deliberately included — it binds real
# TCP listeners and drives real clients over loopback, which is the behaviour
# most likely to differ between platforms.
cross_platform_features := "compression,schema-registry,unstable-protocol,telemetry,socks5,test-broker,ring"

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
ci: fmt-check clippy check api-parity test-ring test minimal-features doc
    @echo ""
    @echo "✓ ci passed — Docker suites not included, run 'just integration' for those"

# Everything, including the Docker-backed integration suites. This is what a
# release should be gated on.
[doc("ci + supply chain + Docker integration suites")]
ci-full: ci deny integration
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

# Check that every config-builder option is reachable from the public builder.
#
# An option on a `*ConfigBuilder` that is missing from the corresponding public
# `*Builder` is implemented, documentable and uncallable. Two such gaps shipped
# before this check existed: KIP-848 could not be selected from
# `Consumer::builder()`, and `docs/errors.md` carried a `dead_letter_queue`
# example that did not compile.
[doc("Public builders must expose every config option")]
api-parity:
    python3 xtask/api_parity.py

# Build the docs with warnings denied, matching CI.
doc:
    RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps --all-features

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
