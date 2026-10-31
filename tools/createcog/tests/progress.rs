use std::{
    path::Path,
    process::{Command, Output},
};

fn run(output: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_createcog"))
        .args(["--input", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/uc-small.nc")])
        .arg("--output")
        .arg(output)
        .args([
            "--variable",
            "WBGT",
            "--scale",
            "8",
            "--tile-size",
            "256",
            "--max-zoom",
            "18",
            "--aligned-levels",
            "1",
        ])
        .args(extra)
        // Progress must be independent of log filtering, also when stderr is a pipe.
        .env("RUST_LOG", "off")
        .output()
        .unwrap()
}

#[test]
fn default_progress_reports_weighted_progress_without_preparation_details_to_stderr() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(&directory.path().join("output.tif"), &[]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        !stderr.contains('\r'),
        "Redirected status must be newline-delimited, without terminal redraws: {stderr}"
    );
    for phase in ["Creating COG: 90.0%", "(100.0%)", "Finished; output COG is ready"] {
        assert!(stderr.contains(phase), "Missing {phase:?}: {stderr}");
    }
    for removed_detail in [
        "Preparing input",
        "Preparing VRTs",
        "Writing combined VRT",
        "Preparing input scale/offset decoding",
        "Queued",
        "staging directory:",
        "Warping bands / building overviews",
        "Reading band metadata",
    ] {
        assert!(!stderr.contains(removed_detail), "Unexpected detail {removed_detail:?}: {stderr}");
    }
    assert!(output.stdout.is_empty(), "Operational progress belongs on stderr");
    for metric in ["CPU", "RSS", "MiB", "disk read"] {
        assert!(!stderr.contains(metric), "{stderr}");
    }
    let dataset = gdal::Dataset::open(directory.path().join("output.tif")).unwrap();
    assert!(dataset.raster_count() > 1);
}

#[test]
fn noprogress_suppresses_the_bar_and_status_messages() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(&directory.path().join("output.tif"), &["--noprogress"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    for message in [
        "Starting",
        "Preparing input",
        "Queued",
        "Assembling",
        "CPU",
        "Finished; output COG is ready",
    ] {
        assert!(!stderr.contains(message), "Unexpected status {message:?}: {stderr}");
    }
}

#[test]
fn command_generation_keeps_diagnostic_stdout_free_of_status_messages() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(&directory.path().join("not-created.tif"), &["--gdal-cmd"]);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("Pipeline:"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("Preparing input"));
    assert!(!directory.path().join("not-created.tif").exists());
}
