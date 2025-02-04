set export
# detect the vcpkg triplet based on the system information
VCPKG_DEFAULT_TRIPLET := if os_family() == "windows" {
  "x64-windows-static-release"
  } else if os() == "macos" {
    if arch() == "aarch64" {
      "arm64-osx-release"
    } else { "x64-osx-release" }
  } else {
    "x64-linux-release"
  }
PYTHON_EXE := if os_family() == "windows" {
    "python.exe"
  } else {
    "bin/python3"
  }
VCPKG_DEFAULT_HOST_TRIPLET := VCPKG_DEFAULT_TRIPLET

cargo-config-gen:
  cp .cargo/config.toml.in .cargo/config.toml
  sd @CARGO_VCPKG_TRIPLET@ {{VCPKG_DEFAULT_TRIPLET}} .cargo/config.toml
  sd @PYTHON_EXE@ {{PYTHON_EXE}} .cargo/config.toml

# on mac symlinks need to be created to avoid python lib errors
# see: https://github.com/PyO3/pyo3/issues/4155
pybootstrap:
  pixi install
  -ln -sf ../../.pixi/envs/default/lib/libpython3.12.dylib ./target/debug/libpython3.12.dylib
  -ln -sf ../../.pixi/envs/default/lib/libpython3.12.dylib ./target/release/libpython3.12.dylib

bootstrap: cargo-config-gen
  echo "Bootstrapping vcpkg:{{VCPKG_DEFAULT_TRIPLET}}..."
  cargo vcpkg -v build
  -cp target/vcpkg/installed/x64-windows-static/lib/gdal.lib target/vcpkg/installed/x64-windows-static/lib/gdal_i.lib
  fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- '-l-framework' '-framework'
  -mkdir -p target/data && mkdir -p target/debug && mkdir -p target/release
  fd -g proj.db ./target/vcpkg/installed --exec cp "{}" ./target/data/
  cp ./target/data/proj.db ./target/debug/
  cp ./target/data/proj.db ./target/release/

build_py:
  #!/usr/bin/env fish
  conda activate ./target/conda
  cd ruster && maturin develop && python ./test.py

serve_tiles dir:
  cargo run -p tileserver --release -- --gis-dir {{dir}}

serve_tiles_tui dir:
  cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{dir}}

doc:
  cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --no-deps --all-features --open

docdeps:
  cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features --open

build_debug:
  cargo build --workspace

build_release:
  cargo build --workspace --release

test_debug:
  cargo nextest run --profile ci --workspace --features=serde,gdal-static,arrow,derive,vector

test_release:
  cargo nextest run --profile ci --workspace --release --features=serde,gdal-static,derive,vector

test_release_slow:
  cargo nextest run --profile slow --workspace --release --features=serde,gdal-static,derive,vector

test_debug_py: pybootstrap
  pixi run test_debug

test_release_py: pybootstrap
  pixi run test_release

build: build_release
test: test_release

rasterbench:
  cargo bench --bench rasterops --package=raster

rasterbenchbaseline name:
  cargo bench --bench rasterops --package=raster -- --save-baseline {{name}}
