# The Redpanda release the blocking `integration-redpanda` CI job and
# `just integration-redpanda` run against. This line is the only place the
# pin is recorded: the test harness and the justfile read it, and Dependabot
# moves it (docker ecosystem). It must be an exact `vMAJOR.MINOR.PATCH` tag.
FROM redpandadata/redpanda:v26.2.4
