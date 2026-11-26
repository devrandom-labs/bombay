# Generating Property-Based Tests

Writing the test declaration is the easy part. These are the decisions that make
the difference between a suite that finds bugs and one that just runs.

## Put constraints in the generator

Discarding inputs after generation wastes the budget when most candidates are
rejected. Generate values within the required bounds instead.

Reserve rejection for conditions you cannot express as a generator — a
relationship between two already-generated values, usually.

Build compound inputs and derive dependent fields from earlier values instead of
generating them independently and filtering.

## Pin the edge cases you already know about

Retain known boundary cases as deterministic regression tests alongside generated
tests so they run every time.

Empty, single-element, all-duplicates, zero, negative, and the maximum representable
value are the ones that recur.

## Settings

Choose explicit case counts and resource bounds for local iteration, CI and longer
campaigns. Use the selected Rust library's documented settings. Do not copy another
library's defaults or confuse machine-dependent timing with a behavior defect.

## Determinism as a property

`f(x) == f(x)` is a tautology for a pure function and a real test for anything else.
Serializers over dicts or sets, anything involving hashing, iteration order, or time —
those can and do return different output for the same input. Assert it where a broken
implementation could falsify it, and not otherwise.

## Testing the error path

Generate arbitrary bytes for a decoder. Check that it succeeds or returns its
documented typed failure without panic or unbounded work. Do not hide unexpected
failures.
