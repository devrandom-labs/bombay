"""Acceptance controls for the compiler transcript receipt."""

import copy
import unittest

from diagnostics import diagnostic_coverage


CHILD = "public_run_blocking_recovers_real_build_error_in_limited_child"
PARENT = "public_run_blocking_recovers_real_build_error_without_limiting_other_tests"
MANIFEST = {
    "cases": ["tests/compile/pass/actor_facade_full.rs",
              "tests/compile/fail/actor_bad_arity.rs"],
    "suites": ["actor_facade_compile_contract"],
    "targets": {"outer_authoring": 1, "run_with": 2, "node_borrowing": 0},
    "ignored": {"run_with": {CHILD: PARENT}},
}
TRANSCRIPT = f"""     Running tests/outer_authoring.rs (/target/outer_authoring-123)
running 1 test
test tests/compile/pass/actor_facade_full.rs [should pass] ... ok
test tests/compile/fail/actor_bad_arity.rs [should fail to compile] ... ok
test actor_facade_compile_contract ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;
     Running tests/run_with.rs (/target/run_with-456)
running 3 tests
test {CHILD} ... ignored, only the parent invokes this test in its independently descriptor-limited child
test {PARENT} ... ok
test public_run_with_returns_work_output ... ok
test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out;
     Running tests/node_borrowing.rs (/target/node_borrowing-789)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;
"""


class DiagnosticCoverage(unittest.TestCase):
    def test_accepts_the_named_child_with_its_parent_and_exact_target_counts(self):
        coverage = diagnostic_coverage(TRANSCRIPT, MANIFEST)
        self.assertTrue(coverage["coverage_complete"])

    def test_accepts_a_corpus_without_ignored_tests(self):
        manifest = copy.deepcopy(MANIFEST)
        manifest["ignored"] = {}
        child_line = next(line for line in TRANSCRIPT.splitlines(True)
                          if line.startswith(f"test {CHILD} ..."))
        transcript = TRANSCRIPT.replace(child_line, "").replace("1 ignored", "0 ignored").replace("running 3 tests", "running 2 tests")
        coverage = diagnostic_coverage(transcript, manifest)
        self.assertTrue(coverage["coverage_complete"])

    def test_ignored_reason_is_not_authority(self):
        transcript = TRANSCRIPT.replace(
            ", only the parent invokes this test in its independently descriptor-limited child", "")
        coverage = diagnostic_coverage(transcript, MANIFEST)
        self.assertTrue(coverage["coverage_complete"])

    def test_rejects_corrupted_transcripts(self):
        child_line = next(line for line in TRANSCRIPT.splitlines(True)
                          if line.startswith(f"test {CHILD} ..."))
        positive = "test tests/compile/pass/actor_facade_full.rs [should pass] ... ok\n"
        negative = "test tests/compile/fail/actor_bad_arity.rs [should fail to compile] ... ok\n"
        corruptions = {
            "wrong ignored name": TRANSCRIPT.replace(CHILD, "unexpected_child"),
            "missing ignored child": TRANSCRIPT.replace(child_line, ""),
            "duplicate ignored child": TRANSCRIPT.replace(child_line, child_line * 2),
            "extra ignored child": TRANSCRIPT.replace(child_line, child_line + "test unexpected_child ... ignored\n"),
            "child passed instead": TRANSCRIPT.replace(child_line, f"test {CHILD} ... ok\n"),
            "child failed instead": TRANSCRIPT.replace(child_line, f"test {CHILD} ... FAILED\n"),
            "missing parent": TRANSCRIPT.replace(f"test {PARENT} ... ok\n", ""),
            "wrong parent": TRANSCRIPT.replace(PARENT, "unrelated_parent"),
            "duplicate parent": TRANSCRIPT.replace(f"test {PARENT} ... ok\n", f"test {PARENT} ... ok\n" * 2),
            "parent moved to another target": TRANSCRIPT.replace(f"test {PARENT} ... ok\n", "").replace(positive, f"test {PARENT} ... ok\n" + positive),
            "failed outer test": TRANSCRIPT + "test unexpected_runtime_case ... FAILED\n",
            "appended failed summary": TRANSCRIPT + "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out;\n",
            "appended filtered summary": TRANSCRIPT + "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out;\n",
            "appended malformed summary": TRANSCRIPT + "test result: malformed\n",
            "unrecognized target heading": TRANSCRIPT + "     Running tests/unrecognized-target.rs (/target/unrecognized-123)\n",
            "extra mismatching fixture": TRANSCRIPT + "test tests/compile/fail/extra.rs ... mismatch\n",
            "extra failed fixture": TRANSCRIPT + "test tests/compile/fail/extra.rs ... FAILED\n",
            "failed target summary": TRANSCRIPT.replace("test result: ok.", "test result: FAILED.", 1),
            "summary outside any target": "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n" + TRANSCRIPT,
            "missing suite": TRANSCRIPT.replace("test actor_facade_compile_contract ... ok\n", ""),
            "duplicate suite": TRANSCRIPT.replace("test actor_facade_compile_contract ... ok\n", "test actor_facade_compile_contract ... ok\n" * 2),
            "duplicate summary": TRANSCRIPT + "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n",
            "missing fixture": TRANSCRIPT.replace(positive, ""),
            "duplicate fixture": TRANSCRIPT.replace(positive, positive * 2),
            "extra fixture": TRANSCRIPT.replace(positive, positive + "test tests/compile/pass/unknown.rs ... ok\n"),
            "positive compiled as failure": TRANSCRIPT.replace(positive, positive.replace("should pass", "should fail to compile")),
            "negative compiled as success": TRANSCRIPT.replace(negative, negative.replace("should fail to compile", "should pass")),
            "filtered selection": TRANSCRIPT.replace("0 filtered out", "1 filtered out", 1),
            "failed summary": TRANSCRIPT.replace("0 failed", "1 failed", 1),
            "wrong ignored count": TRANSCRIPT.replace("1 ignored", "0 ignored"),
            "wrong passed count": TRANSCRIPT.replace("2 passed", "3 passed"),
            "missing zero-case target": TRANSCRIPT.split("     Running tests/node_borrowing.rs")[0],
            "wrong target": TRANSCRIPT.replace("tests/node_borrowing.rs", "tests/unknown.rs"),
            "duplicate target": TRANSCRIPT + "     Running tests/node_borrowing.rs" + TRANSCRIPT.split("     Running tests/node_borrowing.rs")[1],
            "swapped target counts": TRANSCRIPT.replace("1 passed", "TEMP passed").replace("2 passed", "1 passed").replace("TEMP passed", "2 passed"),
            "ignore moved to another target": TRANSCRIPT.replace(child_line, "").replace(positive, child_line + positive),
        }
        for reason, transcript in corruptions.items():
            with self.subTest(reason=reason):
                coverage = diagnostic_coverage(transcript, MANIFEST)
                self.assertFalse(coverage["coverage_complete"])

    def test_rejects_ignore_declaration_for_an_unexecuted_target(self):
        manifest = copy.deepcopy(MANIFEST)
        manifest["ignored"]["unknown"] = manifest["ignored"].pop("run_with")
        coverage = diagnostic_coverage(TRANSCRIPT, manifest)
        self.assertFalse(coverage["coverage_complete"])


if __name__ == "__main__":
    unittest.main()
