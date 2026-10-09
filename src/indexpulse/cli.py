"""Command line entry points.

`ingest` is the one that matters: a long-lived process the chaos driver starts
and kills, so it takes everything it needs from arguments and keeps no state
that the next start cannot rebuild from the manifest.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from indexpulse.core import Safety
from indexpulse.ingest import IngestConfig, IngestWorker
from indexpulse.ingest.changelog import REVISION_TOPIC

app = typer.Typer(
    add_completion=False,
    help="A changelog-fed index that will not serve a superseded document version.",
)

KNOWN_DEFENCES = ("visibility", "swap", "offsets")


def parse_safety(disabled: str) -> Safety:
    names = [name.strip() for name in disabled.split(",") if name.strip()]
    unknown = sorted(set(names) - set(KNOWN_DEFENCES))
    if unknown:
        raise typer.BadParameter(f"unknown defence {unknown}, expected any of {KNOWN_DEFENCES}")
    return Safety(
        visibility="visibility" not in names,
        atomic_swap="swap" not in names,
        offsets_after_durable="offsets" not in names,
    )


@app.command()
def ingest(
    root: Annotated[Path, typer.Option(help="Segments and the manifest.")],
    store: Annotated[Path, typer.Option(help="The document store.")],
    bootstrap: Annotated[str, typer.Option()] = "localhost:9092",
    topic: Annotated[str, typer.Option()] = REVISION_TOPIC,
    partitions: Annotated[int, typer.Option()] = 4,
    group: Annotated[str, typer.Option()] = "indexpulse-ingest",
    dim: Annotated[int, typer.Option()] = 128,
    seal_size: Annotated[int, typer.Option()] = 2000,
    seal_interval: Annotated[float, typer.Option()] = 1.0,
    compaction: Annotated[bool, typer.Option()] = True,
    until: Annotated[float, typer.Option(help="Seconds to run, or 0 to run until killed.")] = 0.0,
    unsafe: Annotated[str, typer.Option(help="Defences to switch off, comma separated.")] = "",
) -> None:
    """Consume the changelog into segments until stopped."""
    config = IngestConfig(
        root=root,
        store_path=store,
        bootstrap=bootstrap,
        topic=topic,
        partitions=partitions,
        group=group,
        dim=dim,
        seal_size=seal_size,
        seal_interval=seal_interval,
        compaction=compaction,
    )
    IngestWorker(config, parse_safety(unsafe)).run(until=until or None)


@app.command()
def manifest(root: Annotated[Path, typer.Option(help="Segments and the manifest.")]) -> None:
    """Print what the index currently holds."""
    from indexpulse.index.manifest_store import ManifestStore

    current = ManifestStore(root).load()
    typer.echo(f"generation {current.generation}, {len(current.segments)} segments")
    typer.echo(f"vectors held {current.document_count}, changelog position {current.offsets}")
    for ref in current.segments:
        typer.echo(f"  {ref.segment_id}  {ref.kind.value:5}  {ref.count:>9} vectors")


if __name__ == "__main__":
    app()
