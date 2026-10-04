"""The read path: fan out across segments, then resolve versions against the store."""

from indexpulse.search.searcher import Searcher, SearchResult

__all__ = ["SearchResult", "Searcher"]
