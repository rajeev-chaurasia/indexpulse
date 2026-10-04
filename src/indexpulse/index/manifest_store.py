"""Publishing and reading the manifest, which is where atomicity is won or lost.

A manifest is published by writing a complete file beside the live one and
renaming it over the top. Rename is atomic within a directory, so a reader sees
either the whole previous generation or the whole next one, and the segment list
and the changelog position it was produced from can never disagree.

Superseded segment files are not deleted at the moment they leave the manifest.
A reader that loaded the previous generation is still holding them, and on a
system without reference counting across processes, waiting out a grace period
is the cheap way to keep a reader from having a file pulled out from under it.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from pathlib import Path

from indexpulse.core import Manifest, Safety

RETIRE_GRACE_SECONDS = 30.0


class TornManifest(RuntimeError):
    """Read a manifest that was being overwritten in place.

    Impossible when the atomic swap is on, which is the point of recording it
    rather than retrying quietly: the safe run has to be able to show a count of
    zero that the unsafe run does not.
    """


class ManifestStore:
    def __init__(self, root: Path, safety: Safety | None = None) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = root / "manifest.json"
        self.safety = safety or Safety()
        self._retiring: list[tuple[float, Path]] = []

    def load(self) -> Manifest:
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return Manifest(generation=0)
        if not raw:
            raise TornManifest("manifest file was empty")
        try:
            return Manifest.from_bytes(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            raise TornManifest(str(exc)) from exc

    def publish(self, manifest: Manifest, *, retired: Iterable[str] = ()) -> None:
        payload = manifest.to_bytes()
        if self.safety.atomic_swap:
            self._publish_atomic(payload)
        else:
            self._publish_in_place(payload)
        self._retire(retired)

    def _publish_atomic(self, payload: bytes) -> None:
        tmp = self.path.with_name(f"manifest.{os.getpid()}.tmp")
        with open(tmp, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        # Renaming is atomic but the directory entry is not durable until the
        # directory itself is synced, which is what a kill right here would
        # otherwise expose.
        directory = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _publish_in_place(self, payload: bytes) -> None:
        """Overwrite the live manifest, which is the bug the control needs.

        Split into two writes so a concurrent reader meets the half-written file
        within a run of minutes rather than one of hours. The race is real
        either way; only its width is arranged here.
        """
        half = len(payload) // 2
        with open(self.path, "wb") as handle:
            handle.write(payload[:half])
            handle.flush()
            time.sleep(0.002)
            handle.write(payload[half:])

    def _retire(self, retired: Iterable[str]) -> None:
        now = time.time()
        for segment_id in retired:
            for suffix in (".hnsw", ".npy"):
                path = self.root / f"{segment_id}{suffix}"
                if not path.exists():
                    continue
                if self.safety.atomic_swap:
                    self._retiring.append((now + RETIRE_GRACE_SECONDS, path))
                else:
                    path.unlink(missing_ok=True)
        self.sweep()

    def sweep(self) -> None:
        due = [entry for entry in self._retiring if entry[0] <= time.time()]
        for _, path in due:
            path.unlink(missing_ok=True)
        self._retiring = [entry for entry in self._retiring if entry[0] > time.time()]
