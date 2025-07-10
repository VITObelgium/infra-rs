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

unexport VCPKG_ROOT
unexport CONDA_ENV
export VCPKG_OVERLAY_PORTS := join(justfile_directory(), "vcpkg-overlay", "ports")
export VCPKG_FORCE_DOWNLOADED_BINARIES := "1"
export LD_LIBRARY_PATH := if os_family() == "windows" {
  ""
} else if os() == "macos" {
  ""
} else {
  source_directory() / ".pixi/envs/default/lib"
}

cargo-config-gen:
  cp .cargo/config.toml.in .cargo/config.toml
  sd @CARGO_VCPKG_TRIPLET@ {{VCPKG_DEFAULT_TRIPLET}} .cargo/config.toml
  sd @PYTHON_EXE@ {{PYTHON_EXE}} .cargo/config.toml
  sd @WORKSPACE_ROOT@ {{justfile_directory()}} .cargo/config.toml

# on mac symlinks need to be created to avoid python lib errors
# see: https://github.com/PyO3/pyo3/issues/4155
pybootstrap:
  pixi install

# gdal-sys uses pkg-config to find the gdal library
# the gdal.pc file contains shlwapi as link flag for the shlwapi library but this gets ignored
# by the pkg-config crate implementation, so we need to replace it with a format that is picked up by the crate
# Warning: gdal fails to build when zstd is enabled and the CONDA_ENV enrironment variable is set
bootstrap: cargo-config-gen
  echo "Bootstrapping vcpkg:{{VCPKG_DEFAULT_TRIPLET}}..."
  cargo vcpkg -v build
  -cp target/vcpkg/installed/x64-windows-static/lib/gdal.lib target/vcpkg/installed/x64-windows-static/lib/gdal_i.lib
  fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- '-l-framework' '-framework'
  fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- ' shlwapi ' ' -lshlwapi '

build_py:
  #!/usr/bin/env fish
  conda activate ./target/conda
  cd ruster && maturin develop && python ./test.py

create_tiles input output:
    cargo run -p createtiles --release -- --input {{input}} --output {{output}} --tile-size 512

serve_tiles dir:
  cargo run -p tileserver --release -- --gis-dir {{dir}}

serve_tiles_tui dir:
  cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{dir}}

doc RUSTDOCFLAGS="-D warnings":
  cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --no-deps --all-features

docdeps:
  cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features --open

build_debug:
  cargo build --workspace

build_release:
  cargo build --workspace --release

test_debug test_name='' $RUST_LOG="debug":
  cargo nextest run --profile ci --workspace --features=serde,gdal,gdal-static,arrow,derive,vector --no-capture {{test_name}}

test_release test_name='':
  cargo nextest run --profile ci --workspace --release --features=serde,gdal,gdal-static,derive,vector {{test_name}}

test_debug_simd:
  cargo +nightly nextest run --profile ci --workspace --features=simd,serde,gdal,gdal-static,arrow,derive,vector

test_release_simd testfilter:
  cargo +nightly nextest run --profile ci --workspace --release --features=simd,serde,gdal,gdal-static,derive,vector '{{testfilter}}'

test_release_slow :
  cargo nextest run --profile slow --workspace --release --features=serde,gdal,gdal-static,derive,vector

test_debug_py: pybootstrap
  pixi run test_debug

test_release_py: pybootstrap
  pixi run test_release

build: build_release
test test_name='': (test_debug test_name)
test_simd testfilter="": (test_release_simd testfilter)

rasterbench:
  cargo bench --bench rasterops --package=geo

cmapbench:
  cargo +nightly bench --bench colormapping --package=inf --features=simd

simdbench:
  cargo +nightly bench --bench simd --package=geo --features=simd,gdal-static,gdal

nosimdbench:
  cargo +nightly bench --bench simd --package=geo  --features=gdal-static,gdal

rasterbenchbaseline name:
  cargo bench --bench rasterops --package=geo -- --save-baseline {{name}}

tiles2raster zoom tile_size="256":
  cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.vrt?tile_format=vrt&tile_size={{tile_size}}" --zoom {{zoom}} --tile-size={{tile_size}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_{{zoom}}_{{tile_size}}.tif

#cargo run --release -p tiles2raster -- --stats --url "https://testmap.marvintest.vito.be/guppy/tiles/raster/no2_atmo_street-20220101-0000UT/{z}/{x}/{y}.png" --zoom {{zoom}} --coord1 51.26,4.33 --coord2 51.16,4.50 -o test_png_{{zoom}}.tif
pngtiles2raster zoom tile_size="256":
  cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.png?tile_format=float_png" --zoom {{zoom}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_png_{{zoom}}.tif
