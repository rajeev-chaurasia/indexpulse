from __future__ import annotations

from pathlib import Path

from indexpulse.core import Manifest, Safety, SegmentKind, SegmentRef
from indexpulse.index.manifest_store import ManifestStore, TornManifest


def ref(segment_id: str, count: int = 10) -> SegmentRef:
    return SegmentRef(
        segment_id=segment_id, kind=SegmentKind.DELTA, count=count, dim=8, published_at=0.0
    )


def test_missing_manifest_reads_as_generation_zero(tmp_path: Path) -> None:
    assert ManifestStore(tmp_path).load().generation == 0


def test_published_manifest_reads_back(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    manifest = Manifest(generation=0).advanced(
        segments=(ref("s1"),), offsets={0: 42}, written_at=1.0
    )
    store.publish(manifest)
    loaded = store.load()
    assert loaded.generation == 1
    assert loaded.segment_ids == ("s1",)
    assert loaded.offsets == {0: 42}


def test_advancing_carries_the_offsets_with_the_segments(tmp_path: Path) -> None:
    """One manifest holds both, so they cannot disagree after a kill."""
    first = Manifest(generation=0).advanced(segments=(ref("s1"),), offsets={0: 10}, written_at=0.0)
    second = first.advanced(segments=(ref("s1"), ref("s2")), offsets={0: 20}, written_at=1.0)
    assert second.generation == 2
    assert second.offsets == {0: 20}
    assert second.segment_ids == ("s1", "s2")


def test_document_count_sums_the_segments(tmp_path: Path) -> None:
    manifest = Manifest(generation=0).advanced(
        segments=(ref("s1", 10), ref("s2", 5)), offsets={}, written_at=0.0
    )
    assert manifest.document_count == 15


def test_a_truncated_manifest_is_reported_not_guessed(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    store.publish(
        Manifest(generation=0).advanced(segments=(ref("s1"),), offsets={}, written_at=0.0)
    )
    payload = store.path.read_bytes()
    store.path.write_bytes(payload[: len(payload) // 2])
    try:
        store.load()
    except TornManifest:
        return
    raise AssertionError("a half-written manifest was parsed as valid")


def test_retired_segments_outlive_the_swap_that_dropped_them(tmp_path: Path) -> None:
    """A reader on the previous generation is still holding these files."""
    store = ManifestStore(tmp_path, Safety())
    (tmp_path / "old.hnsw").write_bytes(b"x")
    (tmp_path / "old.npy").write_bytes(b"x")
    store.publish(Manifest(generation=0), retired=["old"])
    assert (tmp_path / "old.hnsw").exists()


def test_without_the_atomic_swap_retirement_is_immediate(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path, Safety(atomic_swap=False))
    (tmp_path / "old.hnsw").write_bytes(b"x")
    (tmp_path / "old.npy").write_bytes(b"x")
    store.publish(Manifest(generation=0), retired=["old"])
    assert not (tmp_path / "old.hnsw").exists()
