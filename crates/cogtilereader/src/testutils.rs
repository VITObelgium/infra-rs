use std::path::PathBuf;

use path_macro::path;

pub fn workspace_test_data_dir() -> PathBuf {
    path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data")
}
