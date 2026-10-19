"""Focused staging tests without real conda archives or indexing subprocesses."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import update_channel


class UpdateChannelTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.packages = self.root / "packages"
        self.packages.mkdir()
        self.channel = self.root / "channel"

        metadata_patch = mock.patch.object(
            update_channel,
            "package_index",
            return_value={"name": "sas-raster-tools", "subdir": "linux-64"},
        )
        self.package_index = metadata_patch.start()
        self.addCleanup(metadata_patch.stop)
        run_patch = mock.patch.object(update_channel.subprocess, "run")
        self.run = run_patch.start()
        self.addCleanup(run_patch.stop)

    def make_package(self, relative_path, contents=b"package contents"):
        package = self.packages / relative_path
        package.parent.mkdir(parents=True, exist_ok=True)
        package.write_bytes(contents)
        return package

    def assert_indexed(self):
        self.run.assert_called_once_with(
            [
                sys.executable,
                "-m",
                "conda_index",
                str(self.channel),
                "--no-write-shards",
                "--no-rss",
            ],
            check=True,
        )

    def test_no_packages_rejected_without_creating_channel(self):
        (self.packages / "nested").mkdir()
        (self.packages / "nested" / "README.txt").write_text("not a package")

        with self.assertRaisesRegex(ValueError, "No .conda packages found"):
            update_channel.update_channel(self.packages, self.channel)

        self.assertFalse(self.channel.exists())
        self.package_index.assert_not_called()
        self.run.assert_not_called()

    def test_unsupported_metadata_rejected_without_creating_channel(self):
        package = self.make_package("sas-raster-tools-1.0-0.conda")
        for metadata in (
            {"name": "another-package", "subdir": "linux-64"},
            {"name": "sas-raster-tools", "subdir": "noarch"},
            {"name": "sas-raster-tools", "subdir": "linux-aarch64"},
        ):
            with self.subTest(metadata=metadata):
                self.package_index.return_value = metadata

                with self.assertRaisesRegex(ValueError, "Unexpected package or platform"):
                    update_channel.update_channel(self.packages, self.channel)

                self.package_index.assert_called_with(package)
                self.assertFalse(self.channel.exists())
                self.run.assert_not_called()

    def test_flat_package_placed_using_metadata(self):
        package = self.make_package("sas-raster-tools-1.0-0.conda")
        self.package_index.return_value = {
            "name": "sas-raster-tools",
            "subdir": "osx-arm64",
        }

        update_channel.update_channel(self.packages, self.channel)

        self.package_index.assert_called_once_with(package)
        self.assertEqual(
            (self.channel / "osx-arm64" / package.name).read_bytes(),
            package.read_bytes(),
        )
        self.assertEqual(list(self.channel.rglob("*.conda")), [
            self.channel / "osx-arm64" / package.name,
        ])
        self.assert_indexed()

    def test_nested_workflow_artifacts_placed_using_metadata_for_all_platforms(self):
        filename = "sas-raster-tools-1.0-0.conda"
        sources = {
            self.make_package(f"workflow-linux/download/output/{filename}", b"linux"): "linux-64",
            self.make_package(f"workflow-macos/linux-64/{filename}", b"macos"): "osx-arm64",
            self.make_package(f"workflow-windows/artifacts/{filename}", b"windows"): "win-64",
        }
        self.package_index.side_effect = lambda package: {
            "name": "sas-raster-tools",
            "subdir": sources[package],
        }

        update_channel.update_channel(self.packages, self.channel)

        self.package_index.assert_has_calls([
            mock.call(package) for package in sorted(sources)
        ])
        self.assertEqual(self.package_index.call_count, len(sources))
        for package, subdir in sources.items():
            with self.subTest(subdir=subdir):
                self.assertEqual(
                    (self.channel / subdir / filename).read_bytes(),
                    package.read_bytes(),
                )
                self.assertTrue(package.exists())
        self.assertEqual(
            {path.name for path in self.channel.iterdir()},
            {"linux-64", "osx-arm64", "win-64", "noarch"},
        )
        self.assertEqual(len(list(self.channel.rglob("*.conda"))), 3)
        self.assertEqual(list((self.channel / "noarch").iterdir()), [])
        self.assert_indexed()

    def test_immutable_conflict_rejected_before_any_channel_writes(self):
        fresh = self.make_package("a-new.conda", b"new artifact")
        conflict = self.make_package("z-existing.conda", b"replacement")
        destination = self.channel / "linux-64" / conflict.name
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"original artifact")
        repodata = destination.parent / "repodata.json"
        repodata.write_bytes(b"existing index")
        before = set(self.channel.rglob("*"))

        with self.assertRaisesRegex(FileExistsError, "Refusing to replace"):
            update_channel.update_channel(self.packages, self.channel)

        self.package_index.assert_has_calls([mock.call(fresh), mock.call(conflict)])
        self.assertEqual(set(self.channel.rglob("*")), before)
        self.assertEqual(destination.read_bytes(), b"original artifact")
        self.assertEqual(repodata.read_bytes(), b"existing index")
        self.run.assert_not_called()

    def test_identical_rerun_accepted_without_recopying(self):
        package = self.make_package("sas-raster-tools-1.0-0.conda")
        destination = self.channel / "linux-64" / package.name
        with mock.patch.object(
            update_channel.shutil, "copy2", wraps=update_channel.shutil.copy2
        ) as copy:
            update_channel.update_channel(self.packages, self.channel)
            copy.assert_called_once_with(package, destination)
            self.assert_indexed()
            copy.reset_mock()
            self.run.reset_mock()

            update_channel.update_channel(self.packages, self.channel)

            copy.assert_not_called()
            self.assert_indexed()
        self.assertEqual(destination.read_bytes(), package.read_bytes())

    def test_duplicate_destination_filename_rejected_even_if_identical(self):
        filename = "sas-raster-tools-1.0-0.conda"
        first = self.make_package(f"workflow-a/{filename}")
        second = self.make_package(f"workflow-b/nested/{filename}")

        with self.assertRaisesRegex(ValueError, "Duplicate artifact"):
            update_channel.update_channel(self.packages, self.channel)

        self.package_index.assert_has_calls([mock.call(first), mock.call(second)])
        self.assertFalse(self.channel.exists())
        self.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
