# The Redpanda release the blocking `integration-redpanda` CI job and
# `just integration-redpanda` run against. This line is the only place the
# pin is recorded: the test harness and the justfile read it. A maintainer moves
# it when `REDPANDA_VERSION=latest just integration-redpanda` passes on a newer release. It must be an exact
# `vMAJOR.MINOR.PATCH` tag.
FROM redpandadata/redpanda:v26.2.4
