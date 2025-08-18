test_filter := ''

# detect the vcpkg triplet based on the system information
default_triplet := if os_family() == "windows" {
    "x64-windows-static-release"
    } else if os() == "macos" {
    if arch() == "aarch64" {
        "arm64-osx-release"
    } else { "x64-osx-release" }
    } else {
    "x64-linux-release"
    }
default_target := if os_family() == "windows" {
    "x86_64-pc-windows-msvc"
    } else if os() == "macos" {
    if arch() == "aarch64" {
        "aarch64-apple-darwin"
    } else { "x86_64-apple-darwin" }
    } else {
    "x64-linux-release"
    }
PYTHON_EXE := if os_family() == "windows" {
        "python.exe"
    } else {
        "bin/python3"
    }
VCPKG_DEFAULT_HOST_TRIPLET := default_triplet

set export
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

cargo-config-gen triplet:
    cp .cargo/config.toml.in .cargo/config.toml
    sd @CARGO_VCPKG_TRIPLET@ {{triplet}} .cargo/config.toml
    sd @CARGO_VCPKG_HOST_TRIPLET@ {{VCPKG_DEFAULT_HOST_TRIPLET}} .cargo/config.toml
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
bootstrap triplet=default_triplet target=default_target: (cargo-config-gen triplet)
    echo "Bootstrapping vcpkg:{{triplet}} for {{target}}..."
    cargo vcpkg -v build --target {{target}}
    -cp target/vcpkg/installed/x64-windows-static/lib/gdal.lib target/vcpkg/installed/x64-windows-static/lib/gdal_i.lib
    fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- '-l-framework' '-framework'
    fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- ' shlwapi ' ' -lshlwapi '

build_py:
    #!/usr/bin/env fish
    conda activate ./target/conda
    cd ruster && maturin develop && python ./test.py

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{dir}}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{dir}}

doc RUSTDOCFLAGS="-D warnings":
    cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --no-deps --all-features

docdeps:
    cargo doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features --open

build_debug target=default_target:
    cargo build --workspace --target {{target}}

build_release target=default_target:
    cargo build --workspace  --target {{target}} --release --features=rayon

build_nofeatures target=default_target:
        cargo build --workspace  --target {{target}} --release --no-default-features

build target=default_target: (build_release target)

test_debug target=default_target $RUST_LOG="debug":
    cargo nextest run -v --profile ci --target {{target}} --workspace --features=serde,gdal,gdal-static,arrow,derive,vector,proj4rs --no-capture {{test_filter}}

test_release target=default_target:
    cargo nextest run --profile ci --target {{target}} --workspace --release --features=serde,derive,vector,rayon,proj4rs {{test_filter}}

test_release_verbose target=default_target:
    cargo nextest run --profile ci --target {{target}} --workspace --release --features=serde,derive,vector,rayon,proj4rs --no-capture {{test_filter}}

test_debug_simd target=default_target:
    cargo +nightly nextest run --profile ci --target {{target}} --workspace --features=simd,serde,gdal,gdal-static,arrow,derive,vector {{test_filter}}

test_release_simd target=default_target:
    cargo +nightly nextest run --profile ci --target {{target}} --workspace --release --features=simd,serde,gdal,gdal-static,derive,vector {{test_filter}}

test_release_slow target=default_target:
    cargo nextest run --profile slow --target {{target}} --workspace --release --features=serde,gdal,gdal-static,derive,vector

test_warp target=default_target:
    cargo nextest run  --profile ci --target {{target}} --workspace -p geo --release --features=serde,derive,vector,proj4rs,rayon --no-capture integration_warp

test_debug_py: pybootstrap
    pixi run test_debug

test_release_py: pybootstrap
    pixi run test_release

test: (test_debug default_target)
test_ci target=default_target: (test_release target)
test_simd target=default_target: (test_release_simd target)

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

create_tiles input output:
    cargo run -p creatembtiles --release -- --input {{input}} --output {{output}} --tile-size 512

tiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.vrt?tile_format=vrt&tile_size={{tile_size}}" --zoom {{zoom}} --tile-size={{tile_size}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_{{zoom}}_{{tile_size}}.tif

#cargo run --release -p tiles2raster -- --stats --url "https://testmap.marvintest.vito.be/guppy/tiles/raster/no2_atmo_street-20220101-0000UT/{z}/{x}/{y}.png" --zoom {{zoom}} --coord1 51.26,4.33 --coord2 51.16,4.50 -o test_png_{{zoom}}.tif
pngtiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.png?tile_format=float_png" --zoom {{zoom}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_png_{{zoom}}.tif
