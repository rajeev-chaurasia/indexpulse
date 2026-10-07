"""The changelog, and why the broker's own offset tracking is not used.

Kafka will happily remember how far a consumer group has read. Relying on that
would give the system two records of the same fact, the broker's and the
manifest's, and after a kill they disagree: the broker knows what was delivered,
the manifest knows what was made durable. Only the second one is true.

So partitions are assigned explicitly and seeks come from the manifest. Consumer
group state is still created, because the broker insists, and is never read.
"""

from __future__ import annotations

from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from indexpulse.core import Revision

REVISION_TOPIC = "indexpulse.revisions"


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        return
    for _, future in admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    ).items():
        future.result()


class ChangelogPublisher:
    def __init__(self, bootstrap: str, topic: str = REVISION_TOPIC) -> None:
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "linger.ms": 5,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 1_000_000,
            }
        )

    def publish(self, revision: Revision) -> None:
        # Keyed by document so every revision of one document lands on one
        # partition and arrives in order. Across documents order does not
        # matter, which is what lets the worker read partitions in parallel.
        self._producer.produce(
            self.topic,
            key=str(revision.doc_id).encode(),
            value=revision.to_bytes(),
        )
        self._producer.poll(0)

    def publish_all(self, revisions: Iterable[Revision]) -> int:
        count = 0
        for revision in revisions:
            self.publish(revision)
            count += 1
        return count

    def flush(self, timeout: float = 60.0) -> int:
        return int(self._producer.flush(timeout))


class ChangelogConsumer:
    def __init__(self, bootstrap: str, topic: str, group: str, partitions: int) -> None:
        self.topic = topic
        self.partitions = partitions
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

    def resume_from(self, offsets: dict[int, int]) -> None:
        """Assign every partition and start where the manifest says to."""
        assignment = [
            TopicPartition(self.topic, partition, offsets.get(partition, 0))
            for partition in range(self.partitions)
        ]
        self._consumer.assign(assignment)

    def poll(self, max_messages: int, timeout: float) -> list[tuple[int, int, Revision]]:
        batch = self._consumer.consume(num_messages=max_messages, timeout=timeout)
        records: list[tuple[int, int, Revision]] = []
        for message in batch:
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"changelog read failed: {error}")
            payload = message.value()
            partition, offset = message.partition(), message.offset()
            if payload is None or partition is None or offset is None:
                # The changelog is not allowed to hold a record without a body
                # or a position, so this is a defect rather than a tombstone to
                # skip quietly.
                raise RuntimeError("changelog delivered a record with no position or body")
            records.append((partition, offset, Revision.from_bytes(payload)))
        return records

    def close(self) -> None:
        self._consumer.close()
