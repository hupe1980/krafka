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
# Requires: just (https://just.systems). A recipe whose optional tool is
# missing skips with an explanation locally and fails in CI (`CI` set).

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# ── Feature sets ────────────────────────────────────────────────────────────
#
# `ring` and `rustls-aws-lc-rs` are additive: with both enabled aws-lc-rs wins
# (see `auth::tls::resolve_crypto_provider`), so `--all-features` is a valid
# configuration and needs no hand-maintained exclusion list.

# Everything except the aws-lc-rs backend, so the default `ring` code paths are
# the ones that actually execute. `cfg(not(feature = "rustls-aws-lc-rs"))` arms
# exist in `auth/tls.rs` and `schema_registry/http.rs` and run nowhere else.
ring_features := "zstd,aws-msk,oauth-oidc,native-tls-roots,tls-encrypted-keys,unstable-protocol,ring"

# Portable subset for macOS and Windows: `ring` needs only the platform C
# compiler, where aws-lc-rs would also need CMake and NASM. `test-broker` is deliberately included — it binds real
# TCP listeners and drives real clients over loopback, which is the behaviour
# most likely to differ between platforms.
cross_platform_features := "oauth-oidc,tls-encrypted-keys,unstable-protocol,test-broker,ring"

# Minimum supported Rust version, mirroring `rust-version` in Cargo.toml.
msrv := "1.95"

# Broker versions the SASL suite runs at: the supported floor and the newest
# 4.3 patch. CI's `integration-sasl` matrix reads this list (`just sasl-matrix`).
sasl_versions := "3.9.0 4.3.1"

# Targets the default feature set must build for with nothing but the C
# compiler `ring` needs.
cross_targets := "x86_64-unknown-linux-musl aarch64-unknown-linux-musl x86_64-pc-windows-gnu"

# The files `just mutants` mutation-tests: high-consequence code (the in-flight
# barrier, producer sequence and identity handling, the transaction gate, fetch
# sessions) where a surviving mutant means an assertion is missing.
mutants_files := "--file src/barrier.rs --file src/producer/engine.rs --file src/producer/identity.rs --file src/producer/gate.rs --file src/consumer/fetch_session.rs"

# Fail instead of skipping when a tool is missing and this runs in CI.
# Usage inside a recipe: `{{require}} <tool> "<install hint>" || exit 0`.
require := "require_tool() { if command -v \"$1\" >/dev/null 2>&1; then return 0; fi; if [ -n \"${CI:-}\" ]; then echo \"✗ $1 is not installed (required in CI). Install with: $2\" >&2; exit 1; fi; echo \"⊘ $1 not installed — skipping. Install with: $2\"; return 1; }; require_tool"

# Default recipe: show what is available.
default:
    @just --list --unsorted

# ── The umbrella recipes ────────────────────────────────────────────────────

# Everything CI runs, except the Docker-backed integration suites.
#
# Ordered cheapest-first so a formatting slip fails in seconds rather than
# after a full test run.
[doc("Everything CI runs (no Docker suites)")]
ci: fmt-check clippy check protocol-parity claims-check fuzz-coverage protocol-reachability secret-debug advisory-floors test-reachability config-reachability version-check ci-job-parity workflow-lint no-c no-otel cancel-safety site-check docs-test test-ring test-aws-lc test sim minimal-features doc
    @echo ""
    @echo "✓ ci passed — Docker suites not included, run 'just integration' for those"

# Everything, including the Docker-backed integration suites: the plain suite
# (CI runs it across Kafka 3.9 → 4.3; `just integration-matrix` locally), the
# SASL suite at every version in `sasl_versions`, and the pinned Redpanda suite.
[doc("ci + supply chain + Docker integration suites")]
ci-full: ci deny integration integration-sasl-matrix integration-redpanda
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

# The aws-lc-rs backend without `--all-features`: other features (`aws-msk`)
# enable rustls's `prefer-post-quantum` through their own dependencies, so only
# a build without them shows that krafka's own feature turns it on.
[doc("TLS tests on the aws-lc-rs backend, without --all-features")]
test-aws-lc:
    cargo test --no-default-features --features "ring,rustls-aws-lc-rs" --lib auth::tls
    cargo test --no-default-features --features "ring,rustls-aws-lc-rs,internal" --test tls_key_exchange_opt_out

# The portable feature subset used on macOS and Windows in CI.
test-cross-platform:
    cargo test --no-default-features --features "{{cross_platform_features}}"

# Build and test the share consumer under the default `ring` backend alone.
[doc("Share consumer tests under the default `ring` backend")]
test-share-consumer:
    cargo test --no-default-features --features "ring,test-broker" --lib share_consumer

# Build the default feature set for musl and windows-gnu targets.
#
# Needs each target installed (`rustup target add`) and a C compiler for it,
# for `ring`; the CI job installs musl-tools and gcc cross compilers.
[doc("Default features build for musl and windows-gnu")]
cross-build:
    #!/usr/bin/env bash
    set -euo pipefail
    for t in {{cross_targets}}; do
        echo "▶ $t"
        cargo build --locked --lib --target "$t"
    done

# The default graph compiles no C except `ring`'s and links no system library,
# on every target; each named feature's C requirement matches its record.
# Resolves from Cargo.lock without building.
[doc("No C in the default build except ring's")]
no-c:
    cargo fetch --locked --quiet
    python3 xtask/no_c.py

# No OpenTelemetry crate in the normal dependency graph under any feature set:
# krafka emits spans through `tracing`, and the bridge belongs to the
# application. The second run plants violations and fails unless caught.
[doc("No OpenTelemetry crate in the dependency graph")]
no-otel:
    cargo fetch --locked --quiet
    python3 xtask/no_otel.py
    python3 xtask/no_otel.py --self-test >/dev/null

# Every direct dependency resolved to the floor declared in Cargo.toml builds,
# with default and with all features. Resolution needs nightly cargo; the
# build uses the default toolchain. Cargo.lock is restored afterwards.
[doc("Declared dependency floors build")]
minimal-versions:
    #!/usr/bin/env bash
    set -euo pipefail
    {{require}} rustup "https://rustup.rs" || exit 0
    if ! rustup run nightly cargo --version >/dev/null 2>&1; then
        if [ -n "${CI:-}" ]; then echo "✗ nightly toolchain required in CI" >&2; exit 1; fi
        echo "⊘ nightly toolchain not installed — skipping."; exit 0
    fi
    cp Cargo.lock "${TMPDIR:-/tmp}/krafka-Cargo.lock.bak"
    trap 'mv "${TMPDIR:-/tmp}/krafka-Cargo.lock.bak" Cargo.lock' EXIT
    cargo +nightly update -Z direct-minimal-versions
    cargo check --lib
    cargo check --lib --all-features

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
# Reads a vendored snapshot, so it needs no network and cannot flake. Fails on
# an API pinned below its stable Kafka ceiling or a version gate missing from
# the table.
[doc("API version table must match the vendored Kafka schema snapshot")]
protocol-parity:
    python3 xtask/protocol_parity.py

# The KIP table, the not-implemented list, the Cargo-feature table and the
# tracked Kafka release are generated from `xtask/kips.toml` and the
# registries; this fails when a generated region was edited by hand, when the
# registries changed without regenerating, or when evidence does not resolve.
# Symbol evidence is also compiled, from `tests/kip_evidence.rs`. The second
# run plants each violation in a scratch copy and fails unless every one is
# caught.
[doc("Generated capability claims match the registries")]
claims-check:
    python3 xtask/claims.py
    python3 xtask/claims.py --self-test >/dev/null

# Rewrite the generated regions and `tests/kip_evidence.rs` from
# `xtask/kips.toml`, `Cargo.toml`, `api_versions!` and the protocol snapshot.
[doc("Regenerate the capability claims")]
gen-claims:
    python3 xtask/claims.py --write

# Every (API, version) pair in `api_versions!` has a fuzz path and a committed
# seed, and every fuzz target has seeds. `--write-seeds` regenerates the
# response-decode seeds after a table change.
[doc("Every negotiated (API, version) is fuzzed and seeded")]
fuzz-coverage:
    python3 xtask/fuzz_coverage.py
    python3 xtask/fuzz_coverage.py --self-test >/dev/null

# No credential-bearing type may derive Debug.
#
# `Debug` is the quiet way secrets reach a log aggregator: a `tracing` field, an
# error context or a panic message that formats the enclosing value is enough.
# Each run first proves itself against the planted violations in
# `xtask/secret_debug_fixtures/`.
[doc("No credential-bearing type may derive Debug")]
secret-debug:
    python3 xtask/secret_debug.py

# No version requirement in Cargo.toml may admit a version with a RustSec
# advisory. `cargo deny` audits our lockfile; a downstream lockfile resolves
# the declared floor instead. Needs the advisory database `cargo deny fetch`
# maintains, and skips without one.
[doc("Declared dependency floors admit no advised version")]
advisory-floors:
    python3 xtask/advisory_floors.py

# No test may assert over its own literals.
#
# A test that re-implements the condition it checks stays green when the guard
# it covers is deleted.
[doc("No test may assert over its own literals")]
test-reachability:
    python3 xtask/test_reachability.py

# Every configuration field is settable from its builder and readable back.
#
# `tests/builder_surface.rs` proves named methods exist; this field-driven
# check proves no declared field is unreachable from its public builder.
[doc("Every config field is settable and readable")]
config-reachability:
    python3 xtask/config_reachability.py

# Every decoded response field is read by client code.
#
# The mirror image of `config-reachability`: a field that is decoded and
# round-tripped but read by nobody never reaches the application.
[doc("Every decoded response field is read")]
protocol-reachability:
    python3 xtask/protocol_reachability.py

# Every place that names krafka's own version must agree with Cargo.toml.
#
# Asserts the invariant rather than searching for the old value, so a place
# that is already stale (e.g. `fuzz/Cargo.lock`) is found too.
[doc("krafka's version is consistent everywhere it appears")]
version-check:
    python3 xtask/version_check.py

# Every check in `just ci` has a CI job, and one required check gates them all.
#
# Every job is in `ci-success`'s needs or declared non-blocking, every workflow
# file is declared with its class, and the aggregator carries `if: always()`
# (GitHub reads a skipped required check as success). The second run plants
# each violation in a scratch copy and fails unless every one is caught.
[doc("Every `just ci` check has a CI job, all gated by one required check")]
ci-job-parity:
    python3 xtask/ci_job_parity.py
    python3 xtask/ci_job_parity.py --self-test >/dev/null

# Every data-path method's rustdoc says what dropping it does, and every
# "cancel safe" is a case in tests/cancel_safety.rs.
[doc("Cancel-safety sections are present, well-formed and tested")]
cancel-safety:
    python3 xtask/cancel_safety.py
    python3 xtask/cancel_safety.py --self-test >/dev/null

# The deterministic simulation: library time, dials and random draws go
# through what a run controls, then the per-PR seed budget of every
# workload, the same-seed determinism check, the paused-clock timer tests and
# the cancel-safety harness. Tokio's `select!` RNG is seedable only under
# `--cfg tokio_unstable`, so this builds into its own target directory.
[doc("Deterministic simulation, per-PR seed budget")]
sim:
    python3 xtask/determinism.py
    python3 xtask/determinism.py --self-test >/dev/null
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/sim" RUSTFLAGS="${RUSTFLAGS:-} --cfg tokio_unstable" \
        cargo test --features test-broker --test simulation --test simulation_timers --test cancel_safety

# Replay one simulation seed, printing its trace.
[doc("Replay one simulation seed")]
sim-replay test seed:
    KRAFKA_SIM_SEED={{seed}} KRAFKA_SIM_TRACE=1 CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/sim" \
        RUSTFLAGS="${RUSTFLAGS:-} --cfg tokio_unstable" \
        cargo test --features test-broker --test simulation -- --exact {{test}} --nocapture

# The long budget: 5000 seeds per workload, disjoint from the per-PR ones,
# then every planted defect in tests/plants/ must be caught again.
[doc("Simulation long budget and planted-defect controls")]
sim-long:
    KRAFKA_SIM_SEEDS=1000..6000 CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/sim" \
        RUSTFLAGS="${RUSTFLAGS:-} --cfg tokio_unstable" \
        cargo test --features test-broker --test simulation
    python3 xtask/plants.py

# Lint every workflow: zizmor, plus SHA-pinned actions, no workflow-level
# write permission and no registry token secret.
[doc("Lint the GitHub workflows")]
workflow-lint:
    #!/usr/bin/env bash
    set -euo pipefail
    python3 xtask/workflow_lint.py
    # The planted-violation controls, when zizmor can run.
    if command -v zizmor >/dev/null 2>&1 || command -v uvx >/dev/null 2>&1; then
        python3 xtask/workflow_lint.py --self-test >/dev/null
    fi

# Structural invariants for the documentation site that Zola cannot see: a
# duplicate nav weight, a page no index links to, a README link to a missing
# page.
[doc("Documentation site structure is sound")]
site-check:
    python3 xtask/site_check.py
    python3 xtask/doc_api.py

# Mutation-test the scoped files (`mutants_files`) and print the survivor
# count. Not part of `ci`: the full set takes hours. `mutants-diff` is for
# local runs on a branch.
[doc("Mutation-test the invariant-dense modules")]
mutants *ARGS:
    #!/usr/bin/env bash
    set -uo pipefail
    {{require}} cargo-mutants "cargo install --locked cargo-mutants" || exit 0
    cargo mutants {{mutants_files}} -j 4 --timeout 120 {{ARGS}} -- --all-features --lib
    status=$?
    python3 xtask/mutants_summary.py mutants.out
    exit $status

# Mutation-test only the mutants in the diff against `base` (within the scoped
# files) and report survivors. Survivors and timeouts are reported, not failed:
# equivalent mutants would make a gate a false-positive generator.
#
#   just mutants-diff origin/main
[doc("Mutation-test the mutants a diff touches (report only)")]
mutants-diff base="origin/main":
    #!/usr/bin/env bash
    set -uo pipefail
    {{require}} cargo-mutants "cargo install --locked cargo-mutants" || exit 0
    diff_file="$(mktemp)"
    git diff "{{base}}...HEAD" > "$diff_file"
    cargo mutants {{mutants_files}} --in-diff "$diff_file" -j 2 --timeout 120 -- --all-features --lib
    status=$?
    python3 xtask/mutants_summary.py mutants.out
    # 0 = all caught, 2 = survivors, 3 = timeouts: reported above. Anything
    # else (bad arguments, the unmutated tree failing its tests) is an error.
    case $status in 0|2|3) exit 0 ;; *) exit $status ;; esac

# Compile the guide snippets marked ```rust,compile.
#
# `doc_api.py` checks that names resolve; only compiling a snippet checks that
# each call has the right shape.
[doc("Documentation snippets compile")]
docs-test:
    python3 xtask/docs_test.py

# Build the documentation site into site/public. Zola fails on a broken
# internal link (`internal_level = "error"`); the output must hold its index
# files and exactly one <h1> per guide.
[doc("Build the documentation site")]
site-build:
    #!/usr/bin/env bash
    set -euo pipefail
    cd site
    zola build
    for f in public/index.html public/sitemap.xml public/robots.txt public/search_index.en.json; do
        test -s "$f" || { echo "✗ site-build: $f is missing or empty" >&2; exit 1; }
    done
    fail=0
    for f in public/docs/*/index.html; do
        n=$(grep -o '<h1' "$f" | wc -l | tr -d ' ')
        if [ "$n" -ne 1 ]; then echo "✗ site-build: $f has $n <h1> elements" >&2; fail=1; fi
    done
    exit $fail

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
    # Again over the private items: `broken_intra_doc_links` is allow-by-default
    # for anything rustdoc does not render, so only this pass checks the links
    # in internal docs.
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
    {{require}} cargo-deny "cargo install --locked cargo-deny" || exit 0
    cargo deny check advisories bans licenses sources

# Integration tests against a real Kafka in Docker.
#
# Each suite starts one broker container for the whole test binary;
# `--test-threads=1` because the tests share it. The harness reads
# `KAFKA_IMAGE`/`KAFKA_VERSION` (default `apache/kafka-native:3.9.0`).
[doc("Integration tests against a real Kafka in Docker")]
integration: (_docker-suite "integration_tests")

# SASL integration tests: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER
# over SASL_PLAINTEXT and SASL_SSL against `apache/kafka:$KAFKA_VERSION`
# (default: the first of `sasl_versions`, the supported floor).
[doc("SASL integration tests against a real Kafka in Docker")]
integration-sasl: (_docker-suite "sasl_integration_tests")

# The SASL suite at every version in `sasl_versions`.
[doc("SASL integration tests at every version in the SASL set")]
integration-sasl-matrix:
    #!/usr/bin/env bash
    set -euo pipefail
    for v in {{sasl_versions}}; do
        echo "▶ SASL suite on Kafka $v"
        KAFKA_VERSION="$v" just integration-sasl
    done

# The SASL version set as a JSON array, for CI's matrix.
sasl-matrix:
    @python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' {{sasl_versions}}

# Integration tests against a real Redpanda in Docker.
#
# Runs the release pinned in tests/redpanda/Dockerfile;
# `REDPANDA_VERSION=latest` runs the current release instead. The image is
# pulled first and its digest printed, so a log names what was tested.
#
#   REDPANDA_VERSION=latest just integration-redpanda
[doc("Integration tests against a real Redpanda in Docker")]
integration-redpanda:
    #!/usr/bin/env bash
    set -euo pipefail
    image="$(just redpanda-image)"
    for i in 1 2 3; do
        docker pull --quiet "$image" && break
        [ "$i" = 3 ] && { echo "✗ could not pull $image" >&2; exit 1; }
        sleep 10
    done
    echo "▶ Redpanda $image ($(docker image inspect --format '{{{{index .RepoDigests 0}}' "$image"))"
    REDPANDA_IMAGE_REF="$image" just _docker-suite redpanda_integration_tests

# The Redpanda image the suite starts: the pin, or `$REDPANDA_VERSION`'s tag.
redpanda-image:
    #!/usr/bin/env bash
    set -euo pipefail
    pinned="$(sed -n 's/^FROM[[:space:]]\{1,\}//p' tests/redpanda/Dockerfile)"
    if [ -n "${REDPANDA_VERSION:-}" ]; then echo "${pinned%:*}:$REDPANDA_VERSION"; else echo "$pinned"; fi

# Run the Docker integration suite against every supported Kafka minor.
#
# 3.9 is the supported floor; 4.3 is the protocol-parity target.
#
#   just integration-matrix                    # the default matrix
#   just integration-matrix "4.2.0 4.3.0"      # a subset
[doc("Integration tests across Kafka 3.9 → 4.3")]
integration-matrix versions="3.9.0 4.0.0 4.1.0 4.2.0 4.3.0":
    #!/usr/bin/env bash
    set -euo pipefail
    for v in {{versions}}; do
        echo "▶ Kafka $v"
        KAFKA_VERSION="$v" just integration
    done
    echo "✓ integration-matrix passed for: {{versions}}"

# Run one Docker-backed test binary, then remove the container it started.
# The shared container outlives the test process, so it is removed by label;
# one left by an interrupted run is removed first.
_docker-suite binary:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker info >/dev/null 2>&1; then
        echo "✗ Docker is not available; integration tests need it." >&2
        exit 1
    fi
    cleanup() { docker ps -aq --filter "label=krafka.test-suite={{binary}}" | xargs -r docker rm -f >/dev/null 2>&1 || true; }
    cleanup
    trap cleanup EXIT
    cargo test --test {{binary}} -- --ignored --test-threads=1

# API changes against the last published release.
#
# Checks the stable surface: every feature except `unstable-protocol`,
# `test-broker` (`krafka::testing`, documented unstable) and `internal`.
# A detected break fails unless CHANGELOG.md's Unreleased section has a
# non-empty Breaking list; from 1.0 a break in a non-major bump always fails.
# A run that cannot complete fails.
[doc("API changes against the last published release must be declared")]
semver-check:
    #!/usr/bin/env bash
    set -euo pipefail
    {{require}} cargo-semver-checks "cargo install --locked cargo-semver-checks" || exit 0
    python3 xtask/semver_gate.py

# Check that the crate still builds on its declared MSRV.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! rustup run "{{msrv}}" cargo --version >/dev/null 2>&1; then
        if [ -n "${CI:-}" ]; then echo "✗ Rust {{msrv}} is not installed (required in CI)" >&2; exit 1; fi
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

# Save the performance reference the regression gate compares against: the
# send-path and consume-path benchmarks, as criterion baseline `gate`.
#
# Run this on a known-good commit, then `just bench-check` after a change.
[doc("Record the benchmark baseline")]
bench-baseline:
    python3 xtask/bench_check.py baseline

# Fail if a benchmark regressed against the saved baseline.
#
# krafka-vs-krafka against the fake broker: no figure here is quotable as
# absolute performance, but a constant harness overhead cancels between runs,
# so a real regression still shows. The baseline is not overwritten, so a
# regression fails every check until fixed or re-baselined, and only the
# measurements this run produced are judged. Not in `just ci` — slow and noisy
# on a shared runner. Run it when touching the send path, the accumulator, the
# codec, or the consume path (fetch, decode, buffering, delivery).
[doc("Fail if a benchmark regressed against the saved baseline")]
bench-check:
    python3 xtask/bench_check.py check

# Run one fuzz target for `time` seconds. Requires nightly and cargo-fuzz.
#
# The working corpus is fuzz/corpus/<target> (not committed); the committed
# seeds in fuzz/seeds/<target> are read as extra input. A crash,
# timeout or out-of-memory exits non-zero and leaves the reproducer in
# fuzz/artifacts/<target>.
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
    corpus="fuzz/corpus/{{target}}"
    mkdir -p "$corpus"
    seeds=()
    [ -d "fuzz/seeds/{{target}}" ] && seeds=("fuzz/seeds/{{target}}")
    # A prebuilt cargo-fuzz defaults to the target it was built for (musl from
    # install-action), where ASan cannot run; build for the toolchain's host.
    host="$(rustc +nightly -vV | sed -n 's/^host: //p')"
    cargo +nightly fuzz run --target "$host" {{target}} "$corpus" ${seeds[@]+"${seeds[@]}"} -- \
        -max_total_time={{time}} -timeout=10 -rss_limit_mb=2048

# List the available fuzz targets.
fuzz-list:
    @ls fuzz/fuzz_targets/*.rs | xargs -n1 basename | sed 's/\.rs$//'

# The fuzz targets as a JSON array, for CI's matrix.
fuzz-matrix:
    @just fuzz-list | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().split()))'

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

# Package the release `.crate` and its CycloneDX SBOM into `dir`, and check
# the SBOM names every package of the default graph. Releases are published
# only by `.github/workflows/publish.yml`, through crates.io Trusted Publishing.
[doc("Package the .crate and its SBOM (release workflow)")]
release-package dir="target/release-artifacts":
    #!/usr/bin/env bash
    set -euo pipefail
    {{require}} cargo-cyclonedx "cargo install --locked cargo-cyclonedx" || exit 0
    v="$(just version)"
    mkdir -p "{{dir}}"
    cargo package --locked
    cp "target/package/krafka-$v.crate" "{{dir}}/"
    cargo cyclonedx --format json --target x86_64-unknown-linux-gnu --override-filename "krafka-$v.cdx"
    mv "krafka-$v.cdx.json" "{{dir}}/"
    python3 xtask/sbom_check.py "{{dir}}/krafka-$v.cdx.json"
    (cd "{{dir}}" && shasum -a 256 "krafka-$v.crate" "krafka-$v.cdx.json")

# crates.io serves the `.crate` given, byte for byte: its digest, the attested
# digest (if given), the downloaded file's and the sparse index `cksum` agree.
[doc("Verify crates.io serves exactly this .crate")]
verify-published version crate attested="":
    python3 xtask/verify_published.py --version "{{version}}" --crate "{{crate}}" {{ if attested != "" { "--attested " + attested } else { "" } }}
