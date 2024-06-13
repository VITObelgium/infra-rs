fn main() {
    if cfg!(feature = "gdal-static") {
        if cfg!(target_os = "macos") {
            println!("cargo:rustc-link-search=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/System/Library/Frameworks/");
            println!("cargo:rustc-link-lib=framework=Security");
            println!("cargo:rustc-link-lib=framework=SystemConfiguration");
        } else if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib=Wbemuuid");
            println!("cargo:rustc-link-lib=Crypt32");
            println!("cargo:rustc-link-lib=Wldap32");
            println!("cargo:rustc-link-lib=Secur32");
        }
    }
}
