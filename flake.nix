{
  description = "duco2mqtt";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";

    pkgs-mod.url = "github:VITO-RMA/nix-pkgs/main";
    pkgs-mod.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      pkgs-mod,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        buildEnv = pkgs-mod.lib.mkBuildEnv system;
        pkgs = buildEnv.pkgsStatic.extend (import rust-overlay);

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
                pkg-config
                cargo-nextest
                nil
                nixfmt-rfc-style
                just
                rustAnalyzer
                rustToolchain
                pkg-mod-gdal
              ];
            };
        };
      }
    );
}
