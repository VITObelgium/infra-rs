use super::*;

fn position(reporter: &ProgressReporter) -> usize {
    reporter.display.lock().bar.lock().pb.counter
}

#[test]
fn completed_band_progress_contributes_ninety_percent() {
    let reporter = ProgressReporter::start(true).unwrap();
    reporter.bands(0.5);
    assert_eq!(position(&reporter), 450);
    reporter.bands(1.0);
    assert_eq!(position(&reporter), 900);
}

#[test]
fn assembly_contributes_the_remaining_ten_percent() {
    let reporter = ProgressReporter::start(true).unwrap();
    reporter.bands(1.0);
    reporter.assembly(0.0);
    assert_eq!(position(&reporter), 900);
    reporter.assembly(0.5);
    assert_eq!(position(&reporter), 950);
    reporter.assembly(1.0);
    assert_eq!(position(&reporter), 999);
    reporter.finish();
    assert_eq!(position(&reporter), 1000);
}

#[test]
fn progress_is_monotonic_and_ignores_nonfinite_band_updates() {
    let reporter = ProgressReporter::start(true).unwrap();
    reporter.bands(0.5);
    for fraction in [0.1, -1.0, f64::NAN, f64::INFINITY] {
        reporter.bands(fraction);
        assert_eq!(position(&reporter), 450);
    }
    reporter.bands(2.0);
    assert_eq!(position(&reporter), 900);
}

#[test]
fn assembly_progress_is_monotonic_and_ignores_nonfinite_updates() {
    let reporter = ProgressReporter::start(true).unwrap();
    reporter.assembly(0.5);
    for fraction in [0.1, -1.0, f64::NAN, f64::INFINITY] {
        reporter.assembly(fraction);
        assert_eq!(position(&reporter), 950);
    }
    reporter.assembly(2.0);
    assert_eq!(position(&reporter), 999);
}

#[test]
fn starting_reporter_does_not_advance_progress() {
    let reporter = ProgressReporter::start(true).unwrap();
    assert_eq!(position(&reporter), 0);
}

#[test]
fn disabled_progress_remains_hidden_and_unchanged() {
    let reporter = ProgressReporter::start(false).unwrap();
    reporter.bands(1.0);
    reporter.assembly(1.0);
    reporter.finish();
    assert_eq!(position(&reporter), 0);
    assert!(reporter.display.lock().bar.lock().pb.disable);
}
