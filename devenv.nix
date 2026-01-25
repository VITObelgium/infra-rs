{
  pkgs,
  inputs,
  lib,
  ...
}:
let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
  version = cargoToml.workspace.package.version;

  # Use pkgs-mod's mkBuildEnv to get properly configured musl/mingw packages
  buildEnv = inputs.pkgs-mod.lib.mkBuildEnv pkgs.system;
  mingwBuildEnv = inputs.pkgs-mod.lib.mkBuildEnvMingwCross pkgs.system { };

  pkgsMusl = buildEnv.pkgsStaticMusl;
  pkgsMingw = mingwBuildEnv.pkgsMingw;

  # Use pkgsStatic.rustPlatform which has musl target built-in
  rustPlatformMusl = pkgs.pkgsStatic.rustPlatform;

  # Cross-compilation setup for MinGW
  pkgsCross = pkgsMingw.pkgsCross.mingwW64;
  rustPlatformMingw = pkgsCross.rustPlatform;

  mkRustTool =
    {
      pname,
      useMusl ? false,
      useMingw ? false,
    }:
    let
      rustPlatform =
        if useMusl then
          rustPlatformMusl
        else if useMingw then
          rustPlatformMingw
        else
          pkgs.rustPlatform;
      buildInputsPkgs =
        if useMusl then
          pkgsMusl
        else if useMingw then
          pkgsMingw
        else
          pkgs;
      targetTriple =
        if useMusl then
          "x86_64-unknown-linux-musl"
        else if useMingw then
          "x86_64-pc-windows-gnu"
        else
          null;

    in
    rustPlatform.buildRustPackage {
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

      buildInputs = with buildInputsPkgs; [
        pkg-mod-openssl
        pkg-mod-gdal
        pkg-mod-proj
      ];

      # Override RUSTFLAGS for musl and mingw
      RUSTFLAGS =
        if useMusl then
          lib.concatStringsSep " " [
            "-Crelocation-model=static"
          ]
        else if useMingw then
          lib.concatStringsSep " " [
            "-Clink-arg=-L${pkgsCross.stdenv.cc.cc}/x86_64-w64-mingw32/lib"
            "-Clink-arg=-static"
            "-Clink-arg=-Wl,-Bstatic"
            "-Clink-arg=-lmcfgthread"
            "-Clink-arg=-Wl,-Bdynamic"
            "-Clink-arg=-lkernel32"
            "-Clink-arg=-lntdll"
            "-Clink-arg=-ladvapi32"
          ]
        else
          "";

      cargoBuildFlags = [
        "-p"
        pname
      ]
      ++ lib.optionals (targetTriple != null) [
        "--target"
        targetTriple
      ];

      cargoTestFlags = [
        "-p"
        pname
      ]
      ++ lib.optionals (targetTriple != null) [
        "--target"
        targetTriple
      ];

      # Only install the specific binary we're building
      postInstall = ''
        # Remove any binaries that aren't the one we want
        for bin in $out/bin/*; do
          if [ "$(basename $bin)" != "${pname}" ]${lib.optionalString useMingw " && [ \"$(basename $bin)\" != \"${pname}.exe\" ]"}; then
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

    musl.module = {
      languages.rust = {
        channel = "nightly";
        targets = [ "x86_64-unknown-linux-musl" ];
      };

      env = {
        ENVIRONMENT = "musl";
        CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "x86_64-unknown-linux-musl-gcc";
      };

      packages = with pkgs.pkgsStatic; [
        stdenv.cc
      ];
    };

    mingw.module = {
      languages.rust = {
        channel = "nightly";
        targets = [ "x86_64-pc-windows-gnu" ];
      };

      env = {
        ENVIRONMENT = "mingw";
        CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
        CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
      };

      packages = [
        pkgsCross.stdenv.cc
        pkgsCross.windows.pthreads
      ];
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
    createcog = mkRustTool { pname = "createcog"; };
    tiles2raster = mkRustTool { pname = "tiles2raster"; };
    tileserver = mkRustTool { pname = "tileserver"; };

    # Static musl binaries
    createcog-musl = mkRustTool {
      pname = "createcog";
      useMusl = true;
    };
    tiles2raster-musl = mkRustTool {
      pname = "tiles2raster";
      useMusl = true;
    };
    tileserver-musl = mkRustTool {
      pname = "tileserver";
      useMusl = true;
    };

    # MinGW binaries for Windows
    createcog-mingw = mkRustTool {
      pname = "createcog";
      useMingw = true;
    };
    tiles2raster-mingw = mkRustTool {
      pname = "tiles2raster";
      useMingw = true;
    };
    tileserver-mingw = mkRustTool {
      pname = "tileserver";
      useMingw = true;
    };
  };

}
