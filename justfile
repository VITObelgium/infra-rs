devenv_nightly := "devenv --option devenv.warnOnNewVersion:bool false --profile nightly shell -- bash -euc"
devenv_musl := "devenv --option devenv.warnOnNewVersion:bool false --profile musl shell -- bash -euc"
devenv_musl_aarch64 := "devenv --option devenv.warnOnNewVersion:bool false --profile musl-aarch64 shell -- bash -euc"
devenv_musl_x86_64 := "devenv --option devenv.warnOnNewVersion:bool false --profile musl-x86_64 shell -- bash -euc"

[windows]
bootstrap:
    mise -E vcpkg bootstrap

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{ dir }}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{ dir }}

doc:
    cargo doc --workspace --exclude=infra-rs --exclude=vector_derive --no-deps --all-features

docdeps:
    cargo +nightly doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features

[windows]
build_debug:
    mise -E vcpkg build

[unix]
build_debug:
    cargo build -p geo --features=gdal-static

[windows]
build_release:
    mise -E vcpkg build --release

[unix]
build_release:
    cargo build -p geo --release

build_nofeatures:
    cargo build --workspace --release --no-default-features

build_allfeatures:
    cargo build --workspace --release --features=serde,gdal-static,arrow,derive,vector,vector-processing,vector-io-xlsx,vector-io-csv,polars,proj4rs,tui

build: build_release

[windows]
test_debug $RUST_LOG="debug":
    mise -E vcpkg run test

[unix]
test_debug $RUST_LOG="debug":
    cargo nextest run -p geo --features=serde,gdal-static,arrow,derive,vector-processing,vector-io-xlsx,vector-io-csv,polars,rayon

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

wheel:
    @ {{ devenv_musl_aarch64 }} 'cd pypi; uvx --from maturin==1.11.5 maturin build --zig --target aarch64-unknown-linux-musl --release --out dist'
    @ {{ devenv_musl_x86_64 }} 'cd pypi; uvx --from maturin==1.11.5 maturin build --zig --target x86_64-unknown-linux-musl --release --out dist'

[unix]
create_release_tarball tool build_type="":
    #!/usr/bin/env bash
    if [ -z "{{ build_type }}" ]; then
        suffix=""
    else
        suffix="-{{ build_type }}"
    fi

    profile_arg=""
    if [ "{{ build_type }}" = "musl" ]; then
        profile_arg="--profile musl"
    elif [ "{{ build_type }}" = "mingw" ]; then
        profile_arg="--profile mingw"
    fi

    TARGET={{ tool }}${suffix}
    RELEASE_DIR=${TARGET}-release-temp
    echo "Building devenv output: ${TARGET}"
    OUTPUT=$(devenv ${profile_arg} build outputs.${TARGET} 2>&1 | grep "^/nix/store")
    if [ -z "${OUTPUT}" ]; then
        echo "Error: Devenv output could not be detected"
        exit 1
    fi

    echo "Devenv output located at: ${OUTPUT}"

    # Create archive
    mkdir -p ${RELEASE_DIR}
    cp ${OUTPUT}/bin/* ${RELEASE_DIR}/ 2>/dev/null

    (cd ${RELEASE_DIR} && tar -czf ../${TARGET}.tar.gz *)
    rm -rf ${RELEASE_DIR}
    echo "Release tarball created ${TARGET}.tar.gz"
