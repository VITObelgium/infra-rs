{
  pkgs,
  inputs,
  ...
}:
{
  overlays = [
    (inputs.pkgs-mod.lib.mkOverlay {
      static = true;
    })
  ];

  packages = with pkgs; [
    just
    lld
    cargo-nextest
    trivy
    just
    pkg-config
    python313
    python313Packages.pyarrow
    pkg-mod-gdal
    pkg-mod-proj
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };
}
