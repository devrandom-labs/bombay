#!/usr/bin/env bash
# Shell self-tests of architecture/check.sh, in execution order.
# shellcheck disable=SC2034 # The runner reads suites.
suites=(
  step
  space
  gate
  design-facts
  quality-facts
  cards
  gate-times
  watch-parks
  turns
  land
  owner-tools
  owner-guard
  lane
  owner-check
  owner-session
  harness
  owner-registry
  host-series
  diagnostics
)
