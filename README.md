[![Build](https://github.com/VITObelgium/infra-rs/actions/workflows/build.yaml/badge.svg)](https://github.com/VITObelgium/infra-rs/actions/workflows/build.yaml)

This repo contains the shared crates for building geo applications

## Project integration
Add this repository as a git submodule into your project

In your main Cargo.toml specify the vcpkg version you wish to use 
```
[package.metadata.vcpkg]
git = "https://github.com/microsoft/vcpkg"
rev = "10b7a17"
```
Optionally also select the vcpkg triplets you wish to use for your targets
```
[package.metadata.vcpkg.target]
x86_64-pc-windows-msvc = { triplet = "x64-windows-static" }
```

Add infra-rs crates as a local dependency by pointing to the path of the subcrates in the submodule and specify the crate features. 
```
[dependencies]
inf        = { path = "infra-rs/crates/inf", features = ["gdal-static", "serde"] }
raster     = { path = "infra-rs/crates/raster" }
```

It is recommended to also add the crates to your workspace when you expect to make changes in the crates.
```
[workspace]
members = ["infra-rs/crates/inf", "infra-rs/crates/vector"]
```

# Setup development tools
To build infra-rs as a standalone project

## Windows
Install the msvc compiler
download and run `https://win.rustup.rs/x86_64`

## Linux
run `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

And follow onscreen instructions.

## Mac
Install the developer tools
`xcode-select --install`

If the bootstrap fails make sure to use m4 from homebrew
`brew link m4 --force`

### Additional tooling
`cargo install cargo-binstall`
`cargo binstall sd fd-find just cargo-vcpkg cargo-nextest`

### Compile the C++ dependencies
`just bootstrap`