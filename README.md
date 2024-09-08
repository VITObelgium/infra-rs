This repo contains the shared crates for building geo applications

## Project integration
Add this repository as a git submodule into your project

In your main Cargo.toml specify the vcpkg version you wish to use 
```
[package.metadata.vcpkg]
git = "https://github.com/microsoft/vcpkg"
rev = "943c5ef"
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