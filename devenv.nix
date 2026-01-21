{
  pkgs,
  inputs,
  config,
  lib,
  ...
}:
let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
  version = cargoToml.workspace.package.version;

  mkRustTool =
    pname:
    pkgs.rustPlatform.buildRustPackage {
      inherit pname version;
      src = ./.;

      cargoLock = {
        lockFile = ./Cargo.lock;
        outputHashes = {
          "crs-definitions-0.3.1" = "sha256-lHV/aO2uw0VVPah/7cN+/n3CczeIHcp/P72JTlNpO/U=";
          "geozero-0.15.1" = "sha256-9dJm5fqnlczKBk85nuQOaBBaoEBVjdVPmSBtO1bGlnU=";
          "proj4wkt-0.1.0" = "sha256-EXhy17+PoYmhaM0Ip6IzG7g2qNTYlDtUD5ohOP7/mjw=";
          "tiff-0.10.3" = "sha256-cFW1M24M0YkYJ/1sR4pfZAkBBdbfMiX1IjlIC2hCuu4=";
        };
      };

      nativeBuildInputs = with pkgs; [
        pkg-config
      ];

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

      # Only install the specific binary we're building
      postInstall = ''
        # Remove any binaries that aren't the one we want
        for bin in $out/bin/*; do
          if [ "$(basename $bin)" != "${pname}" ]; then
            rm "$bin"
          fi
        done
      '';

      meta = with lib; {
        description = "Workspace tool: ${pname}";
        homepage = "https://github.com/VITO-RMA/infra-rs";
        license = licenses.mit;
      };
    };
in
{
  cachix.pull = [ "geo-overlay" ];

  overlays = [
    (inputs.pkgs-mod.lib.mkOverlay {
      static = true;
    })
  ];

  profiles = {
    nightly.module = {
      languages.rust = {
        channel = "nightly";
        components = [
          "rustc"
          "cargo"
          "rust-src"
          "rust-std"
          "rustfmt"
          "clippy"
          "miri"
        ];
      };
      env.ENVIRONMENT = "nightly";
    };
  };

  languages.rust = {
    enable = true;
    channel = "stable";
  };

  packages = with pkgs; [
    just
    lld
    cargo-nextest
    trivy
    just
    pkg-config
    python313
    python313Packages.pyarrow
    pkg-mod-openssl
    pkg-mod-gdal
    pkg-mod-proj
  ];

  outputs = {
    createcog = mkRustTool "createcog";
    tiles2raster = mkRustTool "tiles2raster";
    tileserver = mkRustTool "tileserver";
  };

}
