{
  pkgs,
  inputs,
  ...
}:
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

}
