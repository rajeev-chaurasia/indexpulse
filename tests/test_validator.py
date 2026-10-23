"""The validator's own negative control.

Its job is to reject a summary that has been edited to say something nicer than
the raw records support. A validator that cannot be made to fail would pass a
doctored repository exactly as readily as an honest one.
"""

from __future__ import annotations

from script.validate_evidence import check_claim, check_control, compare


def test_an_edited_number_is_caught() -> None:
    problems = compare({"blocked": 999_999}, {"blocked": 145_687}, "full-safe")
    assert problems == ["full-safe: blocked published as 999999, recomputed as 145687"]


def test_a_number_the_audit_does_not_produce_is_caught() -> None:
    """An invented field is as much a fabrication as a changed one."""
    problems = compare({"invented": 1}, {}, "full-safe")
    assert problems == ["full-safe: invented is published but the audit does not produce it"]


def test_matching_numbers_pass() -> None:
    assert compare({"blocked": 7}, {"blocked": 7}, "full-safe") == []


def test_timing_is_not_recomputed() -> None:
    """Wall clock cannot be recomputed from the records, so it is not compared."""
    assert compare({"wall_seconds": 1.0}, {"wall_seconds": 2.0}, "full-safe") == []


def clean(**over: object) -> dict[str, object]:
    summary: dict[str, object] = {
        "staleness": {"stale_hits": 0, "deleted_hits": 0},
        "lost_revisions": 0,
        "torn_manifest_reads": 0,
        "missing_segment_reads": 0,
        "defences_disabled": [],
    }
    summary.update(over)
    return summary


def test_a_single_stale_hit_fails_the_claim() -> None:
    assert check_claim(clean(staleness={"stale_hits": 1, "deleted_hits": 0}), "r")


def test_a_single_lost_revision_fails_the_claim() -> None:
    assert check_claim(clean(lost_revisions=1), "r")


def test_a_clean_run_passes_the_claim() -> None:
    assert check_claim(clean(), "r") == []


def test_a_control_that_did_not_fail_is_rejected() -> None:
    """A toothless control is worse than none, because it reads as evidence."""
    weak = {
        "safe": clean(),
        "vis": clean(
            defences_disabled=["visibility"], staleness={"stale_hits": 3, "deleted_hits": 0}
        ),
    }
    assert check_control(weak)


def test_a_control_that_failed_hard_is_accepted() -> None:
    strong = {
        "safe": clean(),
        "vis": clean(
            defences_disabled=["visibility"],
            staleness={"stale_hits": 28_147, "deleted_hits": 5_422},
        ),
        "off": clean(defences_disabled=["offsets"], lost_revisions=5_434),
    }
    assert check_control(strong) == []


def test_a_missing_visibility_control_is_rejected() -> None:
    assert check_control({"safe": clean()})
