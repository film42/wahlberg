//! The derive's error messages, checked against `tests/ui/*.stderr`.
//! Regenerate after an intentional change with `TRYBUILD=overwrite`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_*.rs");
    t.compile_fail("tests/ui/fail_*.rs");
}
