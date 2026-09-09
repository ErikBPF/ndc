default:
    @just --list

# Open the requested tooling profile.
shell profile="duckdb":
    devenv --profile {{ quote(profile) }} shell

# Run repository tests and shell checks.
check:
    devenv --profile duckdb test

# Download and verify the pinned Spark runtime and optional engine/format JARs.
setup:
    devenv --profile spark shell -- ./ndc/run.sh setup
