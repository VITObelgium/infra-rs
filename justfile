bootstrap:
    mise -E vcpkg run bootstrap

bootstrap_py:
    mise -E vcpkg run bootstrap_py

serve_tiles dir:
    cargo run -p tileserver --release -- --gis-dir {{ dir }}

serve_tiles_tui dir:
    cargo run -p tileserver --features=tui --release -- --tui --gis-dir {{ dir }}

doc:
    mise -E vcpkg run doc

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
    mise -E simd run test_simd --release

test_debug_py: bootstrap_py
    mise exec -E vcpkg pixi -- pixi run test_debug

test_release_py: bootstrap_py
    mise exec -E vcpkg pixi -- pixi run test_release

test_integration:
    cargo nextest run --profile integration --release --no-capture --no-default-features --features=serde,gdal,gdal-static,derive,vector-io-xlsx,vector-io-csv,polars,rayon

test_all: test_release test_release_py test_integration test_simd

test: test_debug

test_simd: test_release_simd

build_ci: build_allfeatures

test_ci: test_release

miri:
    cargo +nightly miri test --workspace --features=serde,gdal,gdal-static,arrow,derive,vector,vector-io-xlsx,vector-io-csv,polars,proj4rs

rasterbench:
    cargo bench --bench rasterops --package=geo

cmapbench:
    cargo bench --bench colormapping --package=inf --features=simd

simdbench:
    cargo bench --bench simd --package=geo --features=simd,gdal-static,gdal

nosimdbench:
    cargo +nightly bench --bench simd --package=geo  --features=gdal-static,gdal

rasterbenchbaseline name:
    cargo bench --bench rasterops --package=geo -- --save-baseline {{ name }}

create_tiles input output:
    cargo run -p creatembtiles --release -- --input {{ input }} --output {{ output }} --tile-size 512

tiles2raster zoom tile_size="256":
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.vrt?tile_format=vrt&tile_size={{ tile_size }}" --zoom {{ zoom }} --tile-size={{ tile_size }} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_{{ zoom }}_{{ tile_size }}.tif

# cargo run --release -p tiles2raster -- --stats --url "https://testmap.marvintest.vito.be/guppy/tiles/raster/no2_atmo_street-20220101-0000UT/{z}/{x}/{y}.png" --zoom {{zoom}} --coord1 51.26,4.33 --coord2 51.16,4.50 -o test_png_{{zoom}}.tif
pngtiles2raster zoom:
    cargo run --release -p tiles2raster -- --stats --url "http://localhost:4444/api/1/{z}/{x}/{y}.png?tile_format=float_png" --zoom {{ zoom }} --coord1 50.67,2.52 --coord2 51.50,5.91 -o test_png_{{ zoom }}.tif

projinfo:
    @nix --extra-experimental-features 'nix-command flakes' eval --raw --impure --expr 'let lock = builtins.fromJSON (builtins.readFile ./flake.lock); fetch = name: builtins.fetchTree (lock.nodes.${name}.locked); nixpkgsSrc = fetch "nixpkgs"; pkgsModSrc = fetch "pkgs-mod"; pkgsModFlakeFile = import (pkgsModSrc + "/flake.nix"); pkgsModOutputs = pkgsModFlakeFile.outputs { self = { outPath = pkgsModSrc; }; nixpkgs = { outPath = nixpkgsSrc; }; }; pkgs = import nixpkgsSrc { system = builtins.currentSystem; overlays = [ (pkgsModOutputs.lib.mkOverlay { static = true; }) ]; }; in pkgs.pkg-mod-proj.outPath'

gdalinfo:
    @nix --extra-experimental-features 'nix-command flakes' eval --raw --impure --expr 'let lock = builtins.fromJSON (builtins.readFile ./flake.lock); fetch = name: builtins.fetchTree (lock.nodes.${name}.locked); nixpkgsSrc = fetch "nixpkgs"; pkgsModSrc = fetch "pkgs-mod"; pkgsModFlakeFile = import (pkgsModSrc + "/flake.nix"); pkgsModOutputs = pkgsModFlakeFile.outputs { self = { outPath = pkgsModSrc; }; nixpkgs = { outPath = nixpkgsSrc; }; }; pkgs = import nixpkgsSrc { system = builtins.currentSystem; overlays = [ (pkgsModOutputs.lib.mkOverlay { static = true; }) ]; }; in pkgs.pkg-mod-gdal.outPath'

nixinfo:
    just projinfo

    just gdalinfo
