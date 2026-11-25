import io
import subprocess
import unittest
from unittest.mock import patch

from import_kind_images import import_image


class ExactPlatformImportTests(unittest.TestCase):
    def test_rejects_an_index_digest_before_export(self):
        with (
            patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "sha256:platform\n")),
            patch.object(subprocess, "Popen") as export,
        ):
            with self.assertRaisesRegex(RuntimeError, "selects sha256:platform.*expected sha256:index"):
                import_image("kind-node", "linux/amd64", "manager:dev@sha256:index")

        export.assert_not_called()

    def test_rejects_a_tag_that_changes_between_inspect_and_export(self):
        with (
            patch.object(subprocess, "check_output", return_value="docker.io/library/manager:dev manifest sha256:changed\n"),
            patch.object(subprocess, "Popen") as export,
            patch.object(subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 0, "sha256:expected\n"),
                subprocess.CompletedProcess([], 0),
            ]) as run,
        ):
            export.return_value.stdout = io.BytesIO()
            export.return_value.wait.return_value = 0
            export.return_value.poll.return_value = 0
            with self.assertRaisesRegex(RuntimeError, "resolves to sha256:changed.*expected sha256:expected"):
                import_image("kind-node", "linux/amd64", "manager:dev@sha256:expected")

        self.assertEqual(export.call_args.args[0][-1], "docker.io/library/manager:dev")
        self.assertEqual(run.call_count, 2)

    def test_imports_a_bare_manifest_digest_visible_only_through_a_local_tag(self):
        pinned = "docker.io/library/manager@sha256:expected"
        tag = "docker.io/library/manager:dev"
        with (
            patch.object(subprocess, "check_output", side_effect=[
                "manager:old\nmanager:dev\n",
                f"{tag} manifest sha256:expected\n",
                f"{pinned} manifest sha256:expected\n",
            ]),
            patch.object(subprocess, "Popen") as export,
            patch.object(subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 1, ""),
                subprocess.CompletedProcess([], 0, "sha256:old\n"),
                subprocess.CompletedProcess([], 0, "sha256:expected\n"),
                *[subprocess.CompletedProcess([], 0) for _ in range(3)],
            ]) as run,
        ):
            export.return_value.stdout = io.BytesIO()
            export.return_value.wait.return_value = 0
            export.return_value.poll.return_value = 0
            import_image("kind-node", "linux/amd64", pinned)

        self.assertEqual(export.call_args.args[0][-1], tag)
        self.assertEqual(run.call_args_list[-3].args[0][-4:],
                         ["--base-name", "docker.io/library/manager", "--digests", "-"])
        self.assertEqual(run.call_args_list[-2].args[0][-3:], ["inspecti", "-q", tag])
        self.assertEqual(run.call_args_list[-1].args[0][-3:], ["inspecti", "-q", pinned])

    def test_rejects_a_digest_alias_that_points_at_a_different_manifest(self):
        tag = "docker.io/library/manager:dev"
        pinned = "docker.io/library/manager@sha256:expected"
        with (
            patch.object(subprocess, "check_output", side_effect=[
                f"{tag} manifest sha256:expected\n",
                f"{pinned} manifest sha256:changed\n",
            ]),
            patch.object(subprocess, "Popen") as export,
            patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run,
        ):
            export.return_value.stdout = io.BytesIO()
            export.return_value.wait.return_value = 0
            export.return_value.poll.return_value = 0
            with self.assertRaisesRegex(RuntimeError, "resolves to sha256:changed.*expected sha256:expected"):
                import_image("kind-node", "linux/amd64", tag)

        self.assertEqual(run.call_count, 1)

    def test_bare_index_digest_fails_without_starting_an_export(self):
        with (
            patch.object(subprocess, "check_output", return_value="manager:dev\n"),
            patch.object(subprocess, "Popen") as export,
            patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "sha256:platform\n")),
        ):
            with self.assertRaisesRegex(RuntimeError, "No local image.*sha256:index"):
                import_image("kind-node", "linux/amd64", "manager@sha256:index")

        export.assert_not_called()

    def test_direct_digest_import_still_works_without_inspect_platform_metadata(self):
        pinned = "docker.io/library/manager@sha256:expected"
        with (
            patch.object(subprocess, "check_output", side_effect=["", f"{pinned} manifest sha256:expected\n"]),
            patch.object(subprocess, "Popen") as export,
            patch.object(subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 1, ""),
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0),
            ]) as run,
        ):
            export.return_value.stdout = io.BytesIO()
            export.return_value.wait.return_value = 0
            export.return_value.poll.return_value = 0
            import_image("kind-node", "linux/amd64", pinned)

        self.assertEqual(export.call_args.args[0][-1], pinned)
        self.assertEqual(run.call_args_list[1].args[0][-4:],
                         ["--base-name", "docker.io/library/manager", "--digests", "-"])
