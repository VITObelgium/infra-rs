test_filter := ''

bootstrap:
    mise -E vcpkg run bootstrap

bootstrap_py:
    mise -E vcpkg run bootstrap_py

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{dir}}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{dir}}

doc:
    mise -E vcpkg run doc

docdeps:
    cargo +nightly doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features

build_debug:
    mise -E vcpkg run build

build_release:
    mise -E vcpkg run build --release

build_nofeatures:
    cargo build --workspace --release --no-default-features

build: build_release

tools:
    mise -E vcpkg run build --release --workspace

test_debug $RUST_LOG="debug":
    mise -E vcpkg run test

test_release:
    mise -E vcpkg run test --release

test_debug_simd:
    mise -E vcpkg run test_simd

test_release_simd:
    mise -E simd run test_simd --release

test_debug_py: bootstrap_py
    pixi run test_debug

test_release_py: bootstrap_py
    pixi run test_release

test_warp:
    cargo nextest run  --profile integration -p geo --release --no-default-features --features=gdal-static,proj4rs,rayon --no-capture run_all_warp_integration_tests

test_integration:
    mise -E vcpkg run test_integration

test_all: test_release test_release_py test_integration test_simd

test: test_debug
test_ci: test_release
test_simd: test_release_simd

miri:
    cargo +nightly miri test --workspace --features=serde,gdal,gdal-static,arrow,derive,vector,vector-io-xlsx,vector-io-csv,polars,proj4rs

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
