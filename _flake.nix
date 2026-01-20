{
  description = "infra-rs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    rust-overlay.url = "github:oxalica/rust-overlay/stable";
    flake-utils.url = "github:numtide/flake-utils";
    pkgs-mod.url = "github:VITO-RMA/nix-pkgs/main";
    pkgs-mod.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustChannel = pkgs.rust-bin.stable.latest;
        rustToolchain = rustChannel.default.override {
          extensions = [
            "rust-src"
          ];
        };
        rustAnalyzer = rustChannel.rust-analyzer;
      in
      {
        devShells = {
          default =
            with pkgs;
            mkShell {
              buildInputs = [
                stdenv.cc.cc.lib
                cargo-nextest
                just
                rustAnalyzer
                rustToolchain
                python313
                python313Packages.pyarrow
              ];

              shellHook = ''
                export LD_LIBRARY_PATH=${lib.makeLibraryPath [ stdenv.cc.cc.lib ]}:''${LD_LIBRARY_PATH-}
              '';
            };
        };
      }
    );
}
