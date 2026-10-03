"""Pure model and rules, free of the broker, the store and the vector index.

Nothing here opens a socket or a file. The visibility rule in particular decides
whether a search hit may be served, and keeping it testable without any of the
infrastructure around it is the reason this boundary is checked in CI by
`script/check_layers.py`.
"""

from indexpulse.core.content import CORPUS_TOPICS, content_vector, make_content, topic_query
from indexpulse.core.manifest import Manifest, SegmentKind, SegmentRef
from indexpulse.core.revision import (
    DocumentState,
    Operation,
    Revision,
    apply_revision,
    document_key,
)
from indexpulse.core.safety import Safety
from indexpulse.core.visibility import Rejection, classify

__all__ = [
    "CORPUS_TOPICS",
    "DocumentState",
    "Manifest",
    "Operation",
    "Rejection",
    "Revision",
    "Safety",
    "SegmentKind",
    "SegmentRef",
    "apply_revision",
    "classify",
    "content_vector",
    "document_key",
    "make_content",
    "topic_query",
]
