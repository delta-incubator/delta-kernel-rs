"""Tests for reviewing only an individual stacked PR's changes."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import yaml


REPOSITORY = "delta-io/delta-kernel-rs"
PR_URL = f"https://github.com/{REPOSITORY}/pull"
GITHUB_BASE = "a" * 40
PARENT = "b" * 40
HEAD = "c" * 40


def _load_module():
    path = Path(__file__).parents[1] / "review_scope.py"
    spec = importlib.util.spec_from_file_location("review_scope", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _metadata(body: str = "") -> dict:
    return {
        "number": 2,
        "title": "feat: child change",
        "body": body,
        "base": {"sha": GITHUB_BASE, "ref": "main"},
        "head": {
            "sha": HEAD,
            "ref": "stack/child",
            "repo": {"full_name": "contributor/delta-kernel-rs"},
        },
    }


def _stack_body(parent: str | None = None, boundary: str | None = None) -> str:
    lines = ["## Stacked PR"]
    indent = ""
    if parent is not None:
        lines.append(f"- {parent}")
        indent = "  "
    current = f"{indent}- [**stack/child**]({PR_URL}/2)"
    if boundary is not None:
        current += f" [[Files changed]({PR_URL}/2/files/{boundary}..{HEAD})]"
    lines.append(current + " <- _this PR_")
    lines.append(f"{indent}  - [stack/grandchild]({PR_URL}/3)")
    return "\n".join(lines)


class ReviewScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()
        self.get_json = Mock(
            return_value={"status": "ahead", "merge_base_commit": {"sha": PARENT}}
        )

    def resolve(self, body: str) -> str:
        return self.module.resolve_review_base(_metadata(body), REPOSITORY, self.get_json)

    def test_ordinary_pr_and_stack_root_keep_github_base(self) -> None:
        for body in ("", _stack_body(), f"Related PR: {PR_URL}/1"):
            with self.subTest(body=body):
                self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_pinned_parent_excludes_ancestor_pr_even_when_github_base_is_main(self) -> None:
        body = _stack_body(f"[stack/parent]({PR_URL}/1)", PARENT)

        self.assertEqual(self.resolve(body), PARENT)
        self.get_json.assert_called_once_with(f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}")

    def test_pinned_boundary_supports_unlinked_parent_and_renamed_branch(self) -> None:
        body = _stack_body("stack/unlinked-parent", PARENT)
        body = body.replace("**stack/child**", "**#2: child change**")

        self.assertEqual(self.resolve(body), PARENT)

    def test_parent_lookup_skips_siblings_and_uses_immediate_ancestor(self) -> None:
        body = (
            "## Stacked PR\n"
            f"- [stack/root]({PR_URL}/4)\n"
            f"  - [stack/parent]({PR_URL}/1)\n"
            f"    - [stack/sibling]({PR_URL}/5)\n"
            f"    - [**stack/child**]({PR_URL}/2) <- _this PR_\n"
        )
        self.get_json.side_effect = [
            {"head": {"sha": PARENT}, "merged_at": None},
            {"status": "ahead", "merge_base_commit": {"sha": PARENT}},
        ]

        self.assertEqual(self.resolve(body), PARENT)
        self.assertEqual(self.get_json.call_args_list[0].args, (f"repos/{REPOSITORY}/pulls/1",))

    def test_merged_parent_without_pinned_boundary_uses_github_base(self) -> None:
        self.get_json.return_value = {"merged_at": "2026-10-21T00:00:00Z"}

        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)")), GITHUB_BASE)
        self.get_json.assert_called_once_with(f"repos/{REPOSITORY}/pulls/1")

    def test_other_sections_and_descendant_comparisons_do_not_change_scope(self) -> None:
        body = _stack_body() + (
            f" [[Files changed]({PR_URL}/3/files/{PARENT}..{HEAD})]\n"
            "## Examples\n"
            f"- [stack/parent]({PR_URL}/1)\n"
            f"  - [stack/child]({PR_URL}/2)\n"
        )

        self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_stack_header_with_prefix_is_supported(self) -> None:
        body = _stack_body("stack/parent", PARENT).replace("## Stacked PR", "## prefix Stacked PR")

        self.assertEqual(self.resolve(body), PARENT)

    def test_stale_malformed_and_missing_boundaries_stop_review(self) -> None:
        valid = _stack_body("stack/parent", PARENT)
        cases = (
            (valid.replace(f"..{HEAD}", f"..{'d' * 40}"), "stale or invalid"),
            (valid.replace(f"{PARENT}..", "not-a-sha.."), "stale or invalid"),
            (_stack_body("stack/parent"), "no PR or pinned comparison"),
            (valid + "\n" + valid, "multiple stack entries"),
            (valid.replace(f"{PR_URL}/2)", f"{PR_URL}/20)"), "no entry for the current PR"),
        )
        for body, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                self.resolve(body)
        self.get_json.assert_not_called()

    def test_non_ancestor_parent_stops_review(self) -> None:
        for comparison in (
            {"status": "diverged", "merge_base_commit": {"sha": GITHUB_BASE}},
            {"status": "ahead", "merge_base_commit": {"sha": GITHUB_BASE}},
            {},
        ):
            with self.subTest(comparison=comparison):
                self.get_json.return_value = comparison
                with self.assertRaisesRegex(ValueError, "not an ancestor"):
                    self.resolve(_stack_body("stack/parent", PARENT))

    def test_parent_api_failure_does_not_fall_back_to_cumulative_diff(self) -> None:
        self.get_json.side_effect = RuntimeError("GitHub unavailable")

        with self.assertRaisesRegex(RuntimeError, "GitHub unavailable"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

    def test_workflow_diff_and_all_reviewer_prompts_use_the_resolved_scope(self) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((repo_root / ".github/workflows/ai-review.yml").read_text())
        context_step = next(
            step
            for job in workflow["jobs"].values()
            for step in job["steps"]
            if step.get("id") == "context"
        )
        # Execute the actual workflow from scope resolution through prompt generation.
        script = "read -r base_sha" + context_step["run"].split("read -r base_sha", 1)[1]
        cases = (
            ("", GITHUB_BASE),
            (_stack_body("parent", PARENT), PARENT),
            (_stack_body("parent", PARENT).replace(f"..{HEAD}", f"..{'d' * 40}"), None),
        )
        for body, review_base in cases:
            with self.subTest(review_base=review_base), tempfile.TemporaryDirectory() as directory:
                temp = Path(directory)
                (temp / "pr_meta.json").write_text(json.dumps(_metadata(body)))
                (temp / "pr_history.json").write_text("{}")
                (temp / "pr_review_comments.json").write_text("[]")
                (temp / "github-output").touch()
                (temp / "python3").symlink_to(sys.executable)
                comparison_response = json.dumps(
                    {"status": "ahead", "merge_base_commit": {"sha": PARENT}}
                )
                gh = temp / "gh"
                gh.write_text(
                    f"#!{sys.executable}\n"
                    "import os, pathlib, sys\n"
                    "root = pathlib.Path(os.environ['TEST_ROOT'])\n"
                    "with (root / 'api-calls').open('a') as calls:\n"
                    "    calls.write(sys.argv[2] + '\\n')\n"
                    "if '-H' in sys.argv:\n"
                    "    print('diff --git a/child.rs b/child.rs\\n'\n"
                    "          '--- a/child.rs\\n+++ b/child.rs\\n'\n"
                    "          '@@ -0,0 +1 @@\\n+child_change();')\n"
                    "else:\n"
                    f"    print({comparison_response!r})\n"
                )
                gh.chmod(0o700)
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", script.replace("/tmp/", f"{temp}/")],
                    cwd=repo_root,
                    env={
                        **os.environ,
                        "PATH": f"{temp}:{os.environ['PATH']}",
                        "TEST_ROOT": str(temp),
                        "GITHUB_OUTPUT": str(temp / "github-output"),
                        "OUTPUT_MODE": "artifact",
                        "REPO": REPOSITORY,
                    },
                    capture_output=True,
                    text=True,
                )
                if review_base is None:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("stale or invalid", result.stderr)
                    self.assertFalse((temp / "api-calls").exists())
                    self.assertFalse((temp / "review_prompt.txt").exists())
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (temp / "api-calls").read_text().splitlines()
                self.assertTrue(calls)
                self.assertEqual(set(calls), {f"repos/{REPOSITORY}/compare/{review_base}...{HEAD}"})
                output = (temp / "github-output").read_text()
                self.assertIn(f"base_sha={GITHUB_BASE}\n", output)
                self.assertIn(f"review_base_sha={review_base}\n", output)
                for name in ("review_prompt.txt", "reviewer_context.txt"):
                    prompt = (temp / name).read_text()
                    self.assertIn(f"**Review base SHA:** {review_base}", prompt)
                    self.assertIn(f"**GitHub base SHA:** {GITHUB_BASE}", prompt)
                    self.assertIn("Review only defects introduced by the supplied diff", prompt)
                    self.assertIn("+child_change();", prompt)


if __name__ == "__main__":
    unittest.main()
