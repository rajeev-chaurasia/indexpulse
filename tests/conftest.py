from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from indexpulse.core import Operation, Revision, make_content
from indexpulse.store import DocumentStore

DIM = 32


@pytest.fixture
def store(tmp_path: Path) -> Iterator[DocumentStore]:
    with DocumentStore(tmp_path / "docs") as opened:
        yield opened


def revision(doc_id: int, version: int, topic: int = 0, *, delete: bool = False) -> Revision:
    return Revision(
        doc_id=doc_id,
        version=version,
        operation=Operation.DELETE if delete else Operation.UPSERT,
        content="" if delete else make_content(topic, doc_id * 1000 + version),
        published_at=time.time(),
    )
