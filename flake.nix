{
  description = "Nix packages for the infra-rs command-line tools";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    pkgs-mod.url = "github:VITO-RMA/nix-pkgs/main";
    nixpkgs.follows = "pkgs-mod/nixpkgs";

    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      flake-utils,
      nixpkgs,
      pkgs-mod,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        lib = nixpkgs.lib;
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (pkgs-mod.lib.mkOverlay {
              static = true;
            })
            rust-overlay.overlays.default
          ];
        };

        cargoToml = fromTOML (builtins.readFile ./Cargo.toml);
        version = cargoToml.workspace.package.version;

        # Use the same static GDAL/PROJ/OpenSSL package stack as the release
        # builds in .github/workflows/release.yaml.
        mkTool =
          { pname }:
          pkgs.rustPlatform.buildRustPackage {
            inherit pname version;
            src = ./.;

            cargoDeps =
              (pkgs.rustPlatform.importCargoLock.override {
                fetchgit =
                  args:
                  if args.url == "https://github.com/georust/gdal.git" then
                    # Link to Nix's GDAL, not gdal-src; omit its unused C/C++ submodule.
                    # An archive also avoids platform-dependent Git checkout hashes.
                    pkgs.fetchFromGitHub {
                      owner = "georust";
                      repo = "gdal";
                      inherit (args) rev;
                      hash = args.sha256;
                    }
                  else
                    pkgs.fetchgit args;
              })
                {
                  lockFile = ./Cargo.lock;
                  outputHashes = {
                    # gdal-sys shares this Git source and does not need a separate hash.
                    "gdal-0.19.0" = "sha256-oMiHIi6v5vlVu/gpp5GVK+Y2Rzf9clCG0+OWbHJj9Dc=";
                    "tiff-0.11.3" = "sha256-lwtmCvF6TgtFKLh6BqArS48OjJoiP20IzmMWzAalrNU=";
                  };
                };

            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = with pkgs; [
              pkg-mod-openssl
              pkg-mod-gdal
              pkg-mod-proj
            ];

            cargoBuildFlags = [
              "-p"
              pname
            ];

            cargoTestFlags = [
              "-p"
              pname
            ];

            # Only expose the tool selected by this package output. The
            # workspace also contains libraries and several other binaries.
            postInstall = ''
              for bin in $out/bin/*; do
                if [ "$(basename "$bin")" != "${pname}" ]; then
                  rm "$bin"
                fi
              done
            '';

            meta = {
              description = "infra-rs tool: ${pname}";
              homepage = "https://github.com/VITO-RMA/infra-rs";
              license = lib.licenses.mit;
              mainProgram = pname;
              platforms = lib.platforms.unix;
            };
          };

        tools = {
          createcog = mkTool { pname = "createcog"; };
          tiles2raster = mkTool { pname = "tiles2raster"; };
          tileserver = mkTool { pname = "tileserver"; };
        };

      in
      {
        packages = tools;
      }
    );
}
