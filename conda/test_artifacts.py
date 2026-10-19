"""Test each local conda artifact for the current workspace version."""

import subprocess
import tomllib
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with (root / "Cargo.toml").open("rb") as manifest:
    version = tomllib.load(manifest)["workspace"]["package"]["version"]
packages = sorted((root / "conda" / "output").glob(f"sas-raster-tools-{version}-*.conda"))
if not packages:
    raise SystemExit("No package found for the current version; run the conda-build task first")
for package in packages:
    subprocess.run(["rattler-build", "test", "--package-file", str(package)], check=True)
