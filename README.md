[![Nix build](https://github.com/VITObelgium/infra-rs/actions/workflows/build_nix.yaml/badge.svg)](https://github.com/VITObelgium/infra-rs/actions/workflows/build_nix.yaml)
[![Vcpkg bBuild](https://github.com/VITObelgium/infra-rs/actions/workflows/build_vcpkg.yaml/badge.svg)](https://github.com/VITObelgium/infra-rs/actions/workflows/build_vcpkg.yaml)
[![Docs](https://github.com/VITObelgium/infra-rs/actions/workflows/docs.yaml/badge.svg?branch=main)](https://vitobelgium.github.io/infra-rs)

This repo contains the shared crates for building geo applications

## Project integration
Add this repository as a git submodule into your project

When the gdal feature is required, vcpkg needs to be configured

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

## Setup development tools
A nix devenv configuration is provided for setting up a reproducible development environment.
This is the recommended way to setup the development environment on Linux and MacOS.
Use direnv to automatically load the nix environment when entering the repository or manuelly enter a nix shell with `devenv shell`.

Entering the devenv shell also copies the [Rust best practices agent skill](https://github.com/stolinski/s-stack/tree/main/skills/rust-best-practices),
including its reference chapters, into `.agents/skills/rust-best-practices` for Zed and other agents that support this directory.
The skill source is pinned in `devenv.lock`; update it with `devenv update s-stack` and re-enter the shell.
The generated copy contains project-local files rather than symlinks to the Nix store, avoiding Zed's outside-project permission prompts.
It is ignored by Git and replaced on every shell entry, so local edits to the generated skill are discarded.
Start a new Zed agent conversation after entering the shell to discover the skill.

On Windows or if you don't want to use nix, you can use the mise config to setup the development environment.
Check https://mise.jdx.dev/getting-started.html for installing mise.

When using mise you can compile the C++ dependencies with:
`just bootstrap`

## Building the code and run tests
`just build`
`just test_all`
