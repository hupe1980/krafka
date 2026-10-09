# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting: open the repository's **Security** tab and choose **Report a
vulnerability**. Do not open a public issue for a suspected vulnerability.

You can expect an acknowledgement within 7 days and an assessment, with a fix
plan or a reasoned rejection, within 30 days. Fixes are released as a new
version and disclosed in a GitHub security advisory once the release is out.

## Supported versions

krafka is pre-1.0. Only the latest minor release (`0.y.*` for the newest `y`)
receives security fixes; upgrade to it to get them.

## Verifying a release

Every release is published to crates.io from `.github/workflows/publish.yml`
through crates.io Trusted Publishing, after the tagged commit passed CI and a
maintainer approved the release environment. The workflow attests build
provenance for the exact `.crate` it publishes and for the release's CycloneDX
SBOM, then checks that crates.io serves those bytes.

To verify a downloaded crate (requires the GitHub CLI):

```sh
VERSION=x.y.z   # the release to verify
curl -sSfLO "https://static.crates.io/crates/krafka/krafka-$VERSION.crate"
gh attestation verify "krafka-$VERSION.crate" \
  --repo hupe1980/krafka \
  --signer-workflow hupe1980/krafka/.github/workflows/publish.yml
```

Verification succeeds only for the exact bytes the workflow attested; a crate
with any byte changed fails. The provenance names the tagged commit.

`cargo package` of a tagged commit reproduces the published `.crate`
byte for byte, so you can also check a release without trusting the workflow:

```sh
git clone https://github.com/hupe1980/krafka && cd krafka
git checkout "v$VERSION"
cargo package --no-verify --locked
shasum -a 256 "target/package/krafka-$VERSION.crate"   # equals the crates.io sha256
```

If the release workflow's post-publish check ever finds that crates.io serves
different bytes than were attested, the version is yanked and a new patch
version is released; a published version can never be replaced.
