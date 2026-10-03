"""The manifest: the single record of what is durable in the index.

Two sources of truth for how far the changelog has been consumed is a bug that
only shows up after a crash, so there is exactly one. The segment list and the
changelog position advance together inside one manifest, and a worker resuming
after a kill seeks to the position recorded here rather than to whatever the
broker last heard about. Publishing a new generation is therefore the moment a
batch becomes durable, and it is a single rename.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class SegmentKind(StrEnum):
    """Delta segments arrive small and often, base segments are what they merge into."""

    DELTA = "delta"
    BASE = "base"


class SegmentRef(BaseModel):
    """An immutable segment on disk.

    Immutability is what makes the reader side safe without locking: a reader
    that has loaded a segment id can keep using it while a compaction publishes
    a different set, because no file it holds will ever be rewritten in place.
    """

    model_config = ConfigDict(frozen=True)

    segment_id: str
    kind: SegmentKind
    count: int = Field(ge=0)
    dim: int = Field(gt=0)
    published_at: float


class Manifest(BaseModel):
    """The live segment set plus the changelog position that produced it."""

    model_config = ConfigDict(frozen=True)

    generation: int = Field(ge=0)
    segments: tuple[SegmentRef, ...] = ()
    # Partition to the next offset to read, so a fresh manifest is an empty dict
    # rather than a dict of minus ones.
    offsets: dict[int, int] = Field(default_factory=dict)
    written_at: float = 0.0

    @property
    def segment_ids(self) -> tuple[str, ...]:
        return tuple(ref.segment_id for ref in self.segments)

    @property
    def document_count(self) -> int:
        """Vectors held, which exceeds the live document count until compaction.

        Every edit adds a vector and removes none, so the gap between this and
        the store's live count is the backlog compaction exists to clear.
        """
        return sum(ref.count for ref in self.segments)

    def advanced(
        self,
        *,
        segments: tuple[SegmentRef, ...],
        offsets: dict[int, int],
        written_at: float,
    ) -> Manifest:
        return Manifest(
            generation=self.generation + 1,
            segments=segments,
            offsets=dict(offsets),
            written_at=written_at,
        )

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        return cls.model_validate(json.loads(raw))
