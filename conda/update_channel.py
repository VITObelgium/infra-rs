"""Stage sas-raster-tools packages and index a local conda channel checkout."""

import argparse
import filecmp
import json
import shutil
import subprocess
import sys
from contextlib import closing
from pathlib import Path

from conda_package_streaming.package_streaming import stream_conda_info

PLATFORMS = ("linux-64", "osx-arm64", "win-64")
ROOT = Path(__file__).resolve().parent.parent


def package_index(package):
    with closing(stream_conda_info(package)) as members:
        for archive, member in members:
            if member.name == "info/index.json":
                return json.load(archive.extractfile(member))
    raise ValueError(f"Package has no info/index.json: {package}")


def update_channel(packages, channel):
    artifacts = sorted(packages.rglob("*.conda"))
    if not artifacts:
        raise ValueError(f"No .conda packages found in {packages}")

    staged = {}
    for package in artifacts:
        metadata = package_index(package)
        if metadata["name"] != "sas-raster-tools" or metadata["subdir"] not in PLATFORMS:
            raise ValueError(f"Unexpected package or platform in {package}: {metadata}")
        destination = channel / metadata["subdir"] / package.name
        if destination in staged:
            raise ValueError(f"Duplicate artifact for {destination}: {package}")
        if destination.exists() and not filecmp.cmp(package, destination, shallow=False):
            raise FileExistsError(f"Refusing to replace {destination}; bump the package version or build number")
        staged[destination] = package

    for subdir in (*PLATFORMS, "noarch"):
        (channel / subdir).mkdir(parents=True, exist_ok=True)
    for destination, package in staged.items():
        if not destination.exists():
            shutil.copy2(package, destination)
        print(f"Staged {destination}")

    subprocess.run(
        [sys.executable, "-m", "conda_index", str(channel), "--no-write-shards", "--no-rss"],
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", type=Path, default=ROOT / "conda" / "output")
    parser.add_argument("--channel", type=Path, default=ROOT.parent / "conda-channel")
    args = parser.parse_args()
    update_channel(args.packages.resolve(), args.channel.resolve())


if __name__ == "__main__":
    main()
