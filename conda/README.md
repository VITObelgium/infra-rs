# sas-raster-tools conda package

`sas-raster-tools` contains the `createcog` executable and the `cog-analyzer`
terminal UI. It is built **from source**
using Pixi and conda-forge native dependencies, independently of Nix/devenv and
vcpkg. Supported native build platforms are Linux x86-64, macOS ARM64,
and Windows x86-64.

## Build and test

Run these commands from the repository root in a normal shell, **outside
`devenv shell`**. Pixi 0.82.0 is the version used in CI.

```sh
pixi run --locked -e packaging conda-build
pixi run --locked -e packaging conda-test
```

No prebuilt executable, manually installed Rust toolchain, GDAL, or PROJ is
needed. Windows additionally needs the Visual Studio C++ build tools and Windows
SDK, which are available on the CI runner; the conda compiler activation locates
them.

The `packaging` environment contains the packaging tools. The
`pixi-build-rattler-build` backend creates separate conda **build** and **host**
environments from `recipe.yaml`: Rust 1.95, the C/C++ compilers, Python, Git, and
pkg-config (pkgconf on Windows) are build tools; GDAL, PROJ, and SQLite come from
the host environment.
The existing Pixi Python-testing environment is unchanged.

`conda-build` invokes `pixi publish --target-dir`, which builds a local artifact;
it does **not** upload anything. (`pixi build` is deprecated in Pixi 0.82.) The
result is written to:

```text
conda/output/sas-raster-tools-2026.9.14-<build>.conda
```

`conda-test` installs the package into isolated conda environments. It checks the
help and version output for both tools, GDAL NetCDF driver availability, and conversion of a georeferenced raster
into a Web Mercator COG, including use of PROJ's CRS database. Tests disable PROJ
network access. Native dependencies are runtime conda packages, not libraries
copied from the build machine. End users do not need Python; it is only a build
and test dependency.

## Build details

- Both tools are built using `cargo build --release --locked --package createcog
  --package cog-analyzer --no-default-features`. Existing default Cargo builds
  retain their previous feature set.
- The repository's `.cargo/config.toml` is excluded from the conda source tree,
  avoiding AVX2, static MSVC CRT, and macOS 15-specific release flags.
- GDAL is constrained to 3.12 because `gdal-sys` 0.12 ships pre-generated bindings
  through that version. PROJ must be at least 9.6.2 for `proj-sys` 0.27.
- Windows explicitly uses the build environment's `pkgconf.exe` for both Rust
  dependency discovery and preflight checks. Unlike Windows `pkg-config`, its
  shared-library probes exclude PROJ's private/static curl and TIFF link closure.
  Target-specific pkg-config overrides are cleared so probes use the host prefix.
- Build-script regression tests run with `python -m unittest discover -s conda
  -p test_build.py`; they exercise Windows configuration without a Windows linker.
- The GDAL NetCDF plugin is an explicit runtime dependency. ABI-compatible GDAL,
  PROJ, SQLite, and compiler runtime requirements are supplied by conda run exports.
- The MIT license and aggregated Rust dependency licenses are included.
- The recipe reads the version from `Cargo.toml`. Keep `[package].version` in
  `pixi.toml` in sync when bumping the workspace version. Increment the recipe's
  build number when publishing a new build without changing the version.
- Initial builds download compiler/native conda packages and locked Cargo
  dependencies from their configured channels and upstream registries/Git repos.

## Distribution

The separate [Conda workflow](../.github/workflows/conda.yaml) handles package
builds and channel publishing independently of the native release workflow.
On every push to any branch, its `build-conda` matrix uses only Pixi (no devenv
or vcpkg) to build and test all three platforms. Version tag pushes also build
packages. There are no GitHub artifact upload/download steps, and packages are
**not** attached to GitHub Releases. Branch builds do not publish; their package
files remain on the temporary runner.

On `v*.*.*` tags, each platform job publishes its locally built package directly
to [VITO-RMA/conda-channel](https://github.com/VITO-RMA/conda-channel) after its
tests pass. It copies the package into the platform directory based on embedded
metadata, regenerates the channel index using `conda-index`, then commits and
pushes to the channel repository's `main` branch.

If another platform updates the channel first, the job fetches the new tip,
restages its package, and regenerates the index before retrying the push (up to
five attempts). It never force-pushes. Platforms publish independently: a failed
build does not prevent the other successful platform jobs from publishing.

Existing packages with identical bytes can be staged again. Replacing a package
with different contents under the same filename is rejected: bump the version
or recipe build number instead. This also applies to rebuilding an already
published tag, since new archives may contain different timestamps.

### One-time authentication setup

The source repository's `GITHUB_TOKEN` cannot write to another repository.
Configure a fine-grained personal access token:

1. Select resource owner **VITO-RMA** and grant access only to **conda-channel**.
2. Grant **Contents: Read and write**; no Actions or Workflows write permission is
   needed. Obtain organization approval if required by the organization's policy.
3. Add it to the **infra-rs** repository's Actions secrets as
   **`CONDA_CHANNEL_TOKEN`**.

The token owner must have write access to the channel repo. Its `main` branch
must permit the automated push; branch protection requiring pull requests needs
a separate PR-based publishing flow or an approved automation bypass.
A GitHub App installation token scoped to the channel repo is an alternative;
it can be supplied to the checkout step instead of the stored PAT.

No token belongs in either repository's files. The tagged publishing steps use
the configured `CONDA_CHANNEL_TOKEN` secret directly.

### Install from the channel

After the first successful publication:

```sh
conda install -c https://raw.githubusercontent.com/VITO-RMA/conda-channel/main \
  -c conda-forge sas-raster-tools
createcog --help
cog-analyzer --help
```

The public GitHub raw URL serves the indexed channel directly; GitHub Pages is
not required. Conda-forge supplies the native runtime dependencies.

### Update the local channel checkout

With `../conda-channel` checked out, stage locally built packages and regenerate
its index:

```sh
pixi run --locked -e packaging conda-channel-update
```

This modifies the sibling checkout but does **not** commit or push. Paths can be
overridden, as in CI:

```sh
pixi run --locked -e packaging conda-channel-update \
  --packages conda/output --channel conda/channel
```

Test the staging logic without changing the channel checkout:

```sh
pixi run --locked -e packaging conda-channel-test
```
