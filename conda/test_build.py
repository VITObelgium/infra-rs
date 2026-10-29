"""Test native dependency discovery without building or invoking a Windows linker."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import build


class BuildEnvironmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.prefix = self.root / "host prefix"
        self.build_prefix = self.root / "build prefix"
        self.library = self.prefix / "Library"
        self.pkgconfig = self.library / "lib" / "pkgconfig"
        self.pkgconfig.mkdir(parents=True)
        self.proj_pc = self.pkgconfig / "proj.pc"
        self.proj_pc.write_text(
            "prefix=${pcfiledir}/../..\n"
            "libdir=${prefix}/lib\n"
            "includedir=${prefix}/include\n"
            "Name: PROJ\n"
            "Description: Test PROJ DLL metadata\n"
            "Version: 9.6.2\n"
            "Libs: -L${libdir} -lproj_9\n"
            "Requires.private: libtiff-4 libcurl\n"
            "Libs.private: -lole32 -lshell32\n"
            "Cflags: -I${includedir}\n"
        )
        self.base_env = {
            "PREFIX": str(self.prefix),
            "BUILD_PREFIX": str(self.build_prefix),
            "PKG_VERSION": "2026.9.14",
            "CC": "conda-cc",
        }

    def environment(self, windows=True, extra=None):
        with mock.patch.dict(os.environ, {**self.base_env, **(extra or {})}, clear=True):
            return build.build_environment(self.source, self.prefix, self.build_prefix, windows)

    def test_windows_selects_build_pkgconf_and_dynamic_host_libraries(self):
        env = self.environment()
        self.assertEqual(env["PKG_CONFIG"], str(self.build_prefix / "Library" / "bin" / "pkgconf.exe"))
        self.assertEqual(env["PKG_CONFIG_PATH"], str(self.pkgconfig))
        self.assertEqual(env["PKG_CONFIG_LIBDIR"], str(self.pkgconfig))
        self.assertEqual(env["PKG_CONFIG_ALL_DYNAMIC"], "1")
        self.assertEqual(env["GDAL_LIB_DIR"], str(self.library / "lib"))
        self.assertEqual(env["GDAL_INCLUDE_DIR"], str(self.library / "include"))
        self.assertEqual(env["GDAL_DYNAMIC"], "1")
        self.assertEqual(
            env["BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_msvc"],
            "--target=x86_64-pc-windows-gnu",
        )
        self.assertEqual(env["CARGO_HOME"], str(self.build_prefix / "cargo-home"))
        self.assertEqual(env["LIBCLANG_PATH"], str(self.build_prefix / "Library" / "bin"))
        self.assertEqual(env["CARGO_TARGET_DIR"], str(self.source / "target"))
        # Retain pcfiledir-relative paths and metadata; only fix PROJ's import name.
        contents = self.proj_pc.read_text()
        self.assertIn("prefix=${pcfiledir}/../..", contents)
        self.assertIn("-lproj\n", contents)
        self.assertNotIn("-lproj_9", contents)
        self.assertIn("Requires.private: libtiff-4 libcurl", contents)

    @unittest.skipUnless(shutil.which("clang"), "clang is not installed in the test environment")
    def test_windows_binding_target_uses_unsigned_32_bit_enums(self):
        env = self.environment()
        target = env["BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_msvc"]
        probe = """
            enum GDALDataType { GDT_Unknown = 0, GDT_UInt8 = 1 };
            enum OGRwkbGeometryType { wkbUnknown = 0, wkbPoint25D = 0x80000001 };
            _Static_assert(_Generic((enum GDALDataType)0, unsigned int: 1, default: 0), "unsigned enum");
            _Static_assert(sizeof(enum GDALDataType) == 4, "32-bit enum");
            _Static_assert(_Alignof(enum GDALDataType) == 4, "enum alignment");
            _Static_assert(sizeof(enum OGRwkbGeometryType) == 4, "32-bit geometry enum");
            _Static_assert(wkbPoint25D == 2147483649U, "high-bit geometry constant");
            _Static_assert(sizeof(long) == 4, "Windows long");
            _Static_assert(sizeof(void *) == 8, "64-bit Windows pointer");
            #if !defined(_WIN32) || !defined(_WIN64)
            #error Expected 64-bit Windows headers
            #endif
        """
        subprocess.run(
            [shutil.which("clang"), "--target=x86_64-pc-windows-msvc", target, "-x", "c", "-fsyntax-only", "-"],
            input=probe,
            text=True,
            check=True,
            timeout=10,
        )

    def test_target_specific_and_static_overrides_cannot_escape_host_prefix(self):
        overrides = {
            "PKG_CONFIG": "outer-pkg-config",
            "PKG_CONFIG_x86_64-pc-windows-msvc": "outer-target-pkg-config",
            "PKG_CONFIG_LIBDIR_x86_64_pc_windows_msvc": "outer-libdir",
            "HOST_PKG_CONFIG": "outer-host-pkg-config",
            "TARGET_PKG_CONFIG_PATH": "outer-target-path",
            "PKG_CONFIG_SYSROOT_DIR": "%PREFIX%/elsewhere",
            "PKG_CONFIG_ALL_STATIC": "1",
            "PKG_CONFIG_STATIC": "1",
            "PROJ_STATIC": "1",
            "GDAL_STATIC": "1",
            "GDAL_HOME": "outer-gdal",
            "CARGO_ENCODED_RUSTFLAGS": "outer-flags",
            "CARGO_BUILD_TARGET": "outer-target",
        }
        env = self.environment(extra=overrides)
        for name in overrides:
            if name != "PKG_CONFIG":
                self.assertNotIn(name, env)
        self.assertEqual(env["PKG_CONFIG"], str(self.build_prefix / "Library" / "bin" / "pkgconf.exe"))
        self.assertEqual(env["RUSTFLAGS"], "-C strip=symbols")

    def test_unix_keeps_pkg_config_and_conda_compiler(self):
        with mock.patch.object(build.subprocess, "check_output", return_value="host: aarch64-apple-darwin\n"):
            env = self.environment(windows=False)
        self.assertEqual(env["PKG_CONFIG"], "pkg-config")
        self.assertEqual(env["PKG_CONFIG_PATH"], str(self.prefix / "lib" / "pkgconfig"))
        self.assertEqual(env["CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER"], "conda-cc")
        self.assertEqual(env["LIBCLANG_PATH"], str(self.build_prefix / "lib"))
        self.assertNotIn("GDAL_DYNAMIC", env)
        self.assertNotIn("BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_msvc", env)
        self.assertIn("-lproj_9", self.proj_pc.read_text())

    def test_preflight_and_cargo_use_the_same_selected_executable(self):
        env = self.environment()
        (self.source / "Cargo.toml").write_text('[workspace.package]\nversion = "2026.9.14"\n')
        with (
            mock.patch.dict(os.environ, self.base_env, clear=True),
            mock.patch.object(build.Path, "cwd", return_value=self.source),
            mock.patch.object(build, "build_environment", return_value=env),
            mock.patch.object(build.subprocess, "run") as run,
            mock.patch.object(build.shutil, "copy2") as copy,
        ):
            build.main()
        commands = [call.args[0] for call in run.call_args_list]
        pkg_config = env["PKG_CONFIG"]
        self.assertEqual(commands, [
            [pkg_config, "--atleast-version=9.6.2", "proj"],
            ["cargo", "build", "--release", "--locked", "--package", "createcog", "--package", "cog-analyzer", "--no-default-features", "--features", "gdal/bindgen"],
            ["cargo-bundle-licenses", "--format", "yaml", "--output", "THIRDPARTY.yml"],
        ])
        for call in run.call_args_list:
            self.assertIs(call.kwargs["env"], env)
            self.assertTrue(call.kwargs["check"])
        library = self.library if os.name == "nt" else self.prefix
        suffix = ".exe" if os.name == "nt" else ""
        self.assertEqual(copy.call_args_list, [
            mock.call(self.source / "target" / "release" / f"{tool}{suffix}", library / "bin" / f"{tool}{suffix}")
            for tool in ("createcog", "cog-analyzer")
        ])

    @unittest.skipUnless(shutil.which("pkgconf"), "pkgconf is not installed in the test environment")
    def test_pkgconf_shared_probe_excludes_private_dependency_closure(self):
        pkgconf = shutil.which("pkgconf")
        for name, libs in [("libtiff-4", "-ltiff"), ("libcurl", "-lcurl -lssh2 -lcrypto")]:
            (self.pkgconfig / f"{name}.pc").write_text(
                f"Name: {name}\nDescription: Private dependency fixture\nVersion: 1.0\nLibs: {libs}\n"
            )
        env = self.environment()
        shared = subprocess.check_output([pkgconf, "--libs", "--cflags", "proj"], env=env, text=True, timeout=10)
        static = subprocess.check_output([pkgconf, "--static", "--libs", "--cflags", "proj"], env=env, text=True, timeout=10)
        self.assertIn("-lproj", shared)
        for library in ["-ltiff", "-lcurl", "-lssh2", "-lcrypto", "-lole32", "-lshell32"]:
            self.assertNotIn(library, shared)
            self.assertIn(library, static)


if __name__ == "__main__":
    unittest.main()
