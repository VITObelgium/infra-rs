set export
# detect the vcpkg triplet based on the system information
VCPKG_DEFAULT_TRIPLET := if os_family() == "windows" {
  "x64-windows-static"
  } else if os() == "macos" {
    if arch() == "aarch64" {
      "arm64-osx"
    } else { "x64-osx" }
  } else {
    "x64-linux"
  }
VCPKG_DEFAULT_HOST_TRIPLET := VCPKG_DEFAULT_TRIPLET

cargo-config-gen:
  cp .cargo/config.toml.in .cargo/config.toml
  sd @CARGO_VCPKG_TRIPLET@ {{VCPKG_DEFAULT_TRIPLET}} .cargo/config.toml

bootstrap: cargo-config-gen
  echo "Bootstrapping vcpkg:{{VCPKG_DEFAULT_TRIPLET}}..."
  cargo vcpkg -v build
  -cp target/vcpkg/installed/x64-windows-static/lib/gdal.lib target/vcpkg/installed/x64-windows-static/lib/gdal_i.lib
  fd --base-directory target/vcpkg/installed -g gdal.pc --exec sd -F -- '-l-framework' '-framework'

# last line: copy the python library to the debug folder for rust-analyzer to work
pybootstrap:
  #!/usr/bin/env fish
  conda create -y -p ./target/conda python=3.12
  conda init fish
  conda activate ./target/conda && conda install -y maturin pyarrow
  cp ./target/conda/lib/libpython3.12.dylib ./target/debug/

build_py:
  #!/usr/bin/env fish
  conda activate ./target/conda
  cd ruster && maturin develop && python ./test.py

build_debug:
  cargo build

build_release:
  cargo build --release

doc:
  cargo doc --no-deps --open

test:
  cargo pretty-test --all-features -- --nocapture
