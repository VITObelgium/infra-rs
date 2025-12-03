test_filter := ''

bootstrap:
    mise -E vcpkg run bootstrap

bootstrap_py:
    mise -E vcpkg run bootstrap_py

[windows]
build_debug:
    mise -E vcpkg run build
[windows]
build_release:
    mise -E vcpkg run build --release

[unix]
[private]
build_config config:
    cargo build {{config}} --no-default-features --features gdal-static,serde,derive,raster-io-geotiff,vector-io,rayon
[unix]
build_debug: (build_config '')
[unix]
build_release: (build_config '--release')

[windows]
test_debug $RUST_LOG="debug":
    mise -E vcpkg run test
[windows]
test_release:
    mise -E vcpkg run test --release

[unix]
test_debug: (nix_test_config '')
[unix]
test_release: (nix_test_config '--release')

[windows]
test_warp:
    mise -E vcpkg run test_warp --release

[unix]
test_warp:
    cargo nextest run --release --profile integration -p geo \
        --no-default-features --features=gdal-static,proj4rs,rayon \
        --no-capture run_all_warp_integration_tests

build: build_release
test: test_debug
test_ci: test_release

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{dir}}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{dir}}

doc:
    mise -E vcpkg run doc

docdeps:
    cargo +nightly doc --workspace --exclude='infra-rs' --exclude='vector_derive' --all-features

build_nofeatures:
    cargo build --workspace --release --no-default-features

test_debug_simd:
    mise -E vcpkg run test_simd

test_release_simd:
    mise -E simd run test_simd --release

test_debug_py: bootstrap_py
    mise exec -E vcpkg pixi -- pixi run test_debug

test_release_py: bootstrap_py
    mise exec -E vcpkg pixi -- pixi run test_release

test_integration:
    cargo nextest run --profile integration --release --no-capture --no-default-features \
        --features=serde,gdal,gdal-static,derive,vector-processing,vector-io-xlsx,vector-io-csv,polars,rayon,proj4rs

test_all: test_release test_release_py test_integration test_simd

[private]
nix_test_config config:
    cargo nextest run {{config}} --no-default-features --no-capture --features=gdal-static,serde,derive,raster-io-geotiff,vector-io,polars,rayon

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

createcog input output:
    mise -E vcpkg run createcog --input "{{input}}" --output {{output}}

createmultibandcog input output:
    mise -E vcpkg run createcog --input "{{input}}" --output {{output}} --multi-band

tiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.vrt?tile_format=vrt&tile_size={{tile_size}}" --zoom {{zoom}} --tile-size={{tile_size}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_{{zoom}}_{{tile_size}}.tif

#cargo run --release -p tiles2raster -- --stats --url "https://testmap.marvintest.vito.be/guppy/tiles/raster/no2_atmo_street-20220101-0000UT/{z}/{x}/{y}.png" --zoom {{zoom}} --coord1 51.26,4.33 --coord2 51.16,4.50 -o test_png_{{zoom}}.tif
pngtiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.png?tile_format=float_png" --zoom {{zoom}} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_png_{{zoom}}.tif
