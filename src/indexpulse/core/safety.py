"""Which defences are switched on for a run.

The negative control needs each defence removable on its own, so that a failure
in the unsafe run points at one mechanism rather than at the system in general.
These flags are the only difference between the audited safe run and the audited
unsafe run; corpus, queries and kill schedule are identical.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Safety(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Check a hit's version against the document store before serving it.
    visibility: bool = True
    # Publish a manifest by renaming a complete file over the old one, and leave
    # superseded segment files on disk until readers have had time to move on.
    atomic_swap: bool = True
    # Record the changelog position only after the segment holding that range is
    # on disk and named in a published manifest.
    offsets_after_durable: bool = True

    @property
    def all_enabled(self) -> bool:
        return self.visibility and self.atomic_swap and self.offsets_after_durable

    @property
    def disabled_names(self) -> tuple[str, ...]:
        off = []
        if not self.visibility:
            off.append("visibility")
        if not self.atomic_swap:
            off.append("swap")
        if not self.offsets_after_durable:
            off.append("offsets")
        return tuple(off)
