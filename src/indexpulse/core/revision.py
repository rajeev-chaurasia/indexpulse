"""The changelog record and the document state it produces.

A revision is the only thing that enters the system. The document store, the
segments and the manifest are all derived state, rebuildable by replaying
revisions from a known changelog position, which is what makes recovery after a
killed worker a replay rather than a repair.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class Operation(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


class Revision(BaseModel):
    """One changelog entry: a document at a version, or its removal."""

    model_config = ConfigDict(frozen=True)

    doc_id: int = Field(ge=0)
    version: int = Field(ge=1)
    operation: Operation
    content: str = ""
    published_at: float

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        return cls.model_validate(json.loads(raw))


class DocumentState(BaseModel):
    """What the document store believes about a document right now.

    This is the authority the visibility rule consults. A segment may hold older
    vectors for the same document, and does, until compaction removes them.
    """

    model_config = ConfigDict(frozen=True)

    doc_id: int
    version: int
    deleted: bool
    updated_at: float

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        return cls.model_validate(json.loads(raw))


def apply_revision(state: DocumentState | None, revision: Revision) -> DocumentState | None:
    """Fold a revision into the current state, or return None to ignore it.

    Replay after a crash re-delivers revisions that were already applied, and the
    changelog may deliver two revisions of one document out of order across
    partitions. Dropping anything at or below the stored version makes both cases
    harmless instead of moving a document backwards.
    """
    if state is not None and revision.version <= state.version:
        return None
    return DocumentState(
        doc_id=revision.doc_id,
        version=revision.version,
        deleted=revision.operation is Operation.DELETE,
        updated_at=revision.published_at,
    )


def document_key(doc_id: int) -> bytes:
    """Big-endian so the store's key order matches document order.

    Ordered keys let the audit walk the whole store with one iterator instead of
    1.2 million point lookups.
    """
    return doc_id.to_bytes(8, "big")
