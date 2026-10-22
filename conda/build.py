"""Build raster tools using only the conda build/host prefixes."""

import os
import shutil
import subprocess
import tomllib
from pathlib import Path


def build_environment(source, prefix, build_prefix, windows):
    library = prefix / "Library" if windows else prefix
    env = os.environ.copy()
    # The source filter excludes .cargo/config.toml (AVX2, static CRT, macOS 15).
    # Explicit flags also prevent an outer devenv's Rust flags from leaking in.
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    env["RUSTFLAGS"] = "-C strip=symbols"
    env["CARGO_INCREMENTAL"] = "0"
    env["CARGO_HOME"] = str(build_prefix / "cargo-home")
    env["CARGO_TARGET_DIR"] = str(source / "target")
    env.pop("CARGO_BUILD_TARGET", None)
    for variable in list(env):
        if variable.startswith(("PKG_CONFIG", "HOST_PKG_CONFIG", "TARGET_PKG_CONFIG")):
            env.pop(variable)
    env["PKG_CONFIG_PATH"] = str(library / "lib" / "pkgconfig")
    env["PKG_CONFIG_LIBDIR"] = env["PKG_CONFIG_PATH"]
    env["PKG_CONFIG_ALL_DYNAMIC"] = "1"
    for variable in ("GDAL_HOME", "GDAL_STATIC", "PROJ_STATIC"):
        env.pop(variable, None)

    if windows:
        # Unlike Windows pkg-config, pkgconf excludes private link dependencies
        # when probing shared libraries.
        env["PKG_CONFIG"] = str(build_prefix / "Library" / "bin" / "pkgconf.exe")
        env["GDAL_LIB_DIR"] = str(library / "lib")
        env["GDAL_INCLUDE_DIR"] = str(library / "include")
        env["GDAL_DYNAMIC"] = "1"
        # Windows PROJ ships proj.lib, but some builds advertise -lproj_9.
        # Keep the file in place so relative pcfiledir-based prefixes remain valid.
        proj_pc = library / "lib" / "pkgconfig" / "proj.pc"
        proj_pc.write_text(proj_pc.read_text().replace("-lproj_9", "-lproj"))
    else:
        env["PKG_CONFIG"] = "pkg-config"
        rust_info = subprocess.check_output(["rustc", "-vV"], env=env, text=True)
        host = next(line.removeprefix("host: ") for line in rust_info.splitlines() if line.startswith("host: "))
        env[f"CARGO_TARGET_{host.upper().replace('-', '_')}_LINKER"] = env["CC"]
    return env


def main():
    source = Path.cwd()
    prefix = Path(os.environ["PREFIX"])
    windows = os.name == "nt"
    library = prefix / "Library" if windows else prefix
    env = build_environment(source, prefix, Path(os.environ["BUILD_PREFIX"]), windows)

    with (source / "Cargo.toml").open("rb") as manifest:
        version = tomllib.load(manifest)["workspace"]["package"]["version"]
    if version != os.environ["PKG_VERSION"]:
        raise RuntimeError(f"Cargo version {version} does not match package version {os.environ['PKG_VERSION']}")

    # Fail early rather than allowing proj-sys to fall back to building bundled PROJ.
    subprocess.run([env["PKG_CONFIG"], "--atleast-version=9.6.2", "proj"], env=env, check=True)

    subprocess.run(
        ["cargo", "build", "--release", "--locked", "--package", "createcog", "--package", "cog-analyzer", "--no-default-features"],
        env=env,
        check=True,
    )
    subprocess.run(
        ["cargo-bundle-licenses", "--format", "yaml", "--output", "THIRDPARTY.yml"],
        env=env,
        check=True,
    )
    bindir = library / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    for tool in ("createcog", "cog-analyzer"):
        name = f"{tool}.exe" if windows else tool
        shutil.copy2(source / "target" / "release" / name, bindir / name)


if __name__ == "__main__":
    main()
