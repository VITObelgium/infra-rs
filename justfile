devenv_nightly := "devenv --option devenv.warnOnNewVersion:bool false --profile nightly shell -- bash -euc"

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{ dir }}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{ dir }}

doc:
    cargo doc --workspace --exclude=infra-rs --exclude=vector_derive --no-deps --all-features

docdeps:
    cargo +nightly doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features

build_debug:
    cargo build -p geo --features=gdal-static

build_release:
    cargo build -p geo --release

build_nofeatures:
    cargo build --workspace --release --no-default-features

build_allfeatures:
    cargo build --workspace --release --features=serde,gdal-static,arrow,derive,vector,vector-processing,vector-io-xlsx,vector-io-csv,polars,proj4rs,tui

build: build_release

# Build mingw executable and create result symlink
build-mingw output="createcog":
    #!/usr/bin/env bash
    OUTPUT=$(devenv build outputs.{{ output }}-mingw 2>&1 | grep "^/nix/store")
    ln -sfn "$OUTPUT" result-{{ output }}-mingw
    echo "Created symlink: result-{{ output }}-mingw -> $OUTPUT"

test_debug $RUST_LOG="debug":
    cargo nextest run -p geo --features=gdal-static

# The vector processing feature is currently broken, the geozero dependency should be removed
test_release:
    cargo nextest run -p geo --release --features=serde,gdal-static,arrow,derive,vector-processing,vector-io-xlsx,vector-io-csv,polars,rayon

test_debug_simd:
    mise -E vcpkg run test_simd

test_warp:
    mise -E vcpkg run test_warp --release

test_release_simd:
    @ {{ devenv_nightly }} 'set -o pipefail; cargo nextest run --profile ci --release --features=simd,serde,gdal,gdal-static,derive,vector-io-xlsx,vector-io-csv'

test_debug_py:
    @ {{ devenv_nightly }} 'set -o pipefail; cargo nextest run --profile ci --workspace --all-features'

test_release_py:
    cargo nextest run --profile ci --workspace --features=serde,gdal,gdal-static,derive,vector,vector-io-xlsx,rayon,python --release

test_integration:
    cargo nextest run --profile integration --release --no-capture --no-default-features --features=serde,gdal,gdal-static,derive,vector-io-xlsx,vector-io-csv,polars,rayon,proj4rs

test_all: test_release test_release_py test_integration test_simd

test: test_debug

test_simd: test_release_simd

build_ci: build_allfeatures

test_ci: test_all

miri:
    @ {{ devenv_nightly }} 'set -o pipefail; cargo miri test --workspace --features=serde,gdal,gdal-static,arrow,derive,vector,vector-io-xlsx,vector-io-csv,polars,proj4rs'

rasterbench:
    cargo bench --bench rasterops --package=geo

cmapbench:
    cargo bench --bench colormapping --package=inf --features=simd

simdbench:
    @ {{ devenv_nightly }} 'cargo bench --bench simd --package=geo --features=simd,gdal-static,gdal'

rasterbenchbaseline name:
    cargo bench --bench rasterops --package=geo -- --save-baseline {{ name }}

create_tiles input output:
    cargo run -p creatembtiles --release -- --input {{ input }} --output {{ output }} --tile-size 512

tiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.vrt?tile_format=vrt&tile_size={{ tile_size }}" --zoom {{ zoom }} --tile-size={{ tile_size }} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_{{ zoom }}_{{ tile_size }}.tif

pngtiles2raster zoom:
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.png?tile_format=float_png" --zoom {{ zoom }} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_png_{{ zoom }}.tif
