"""Small Kafka helpers used by setup, the load test and the integration test."""
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from ledgerstream import config


def create_topics(topics, bootstrap=config.KAFKA_BOOTSTRAP):
    """topics: dict of name -> partition count. Existing topics are left alone."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(name, num_partitions=p, replication_factor=1)
                                   for name, p in topics.items()])
    for name, f in futures.items():
        try:
            f.result()
        except Exception as e:
            if e.args and e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue
            raise


def delete_topics(names, bootstrap=config.KAFKA_BOOTSTRAP):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for f in admin.delete_topics(list(names)).values():
        f.result()


def group_lag(topic, group, bootstrap=config.KAFKA_BOOTSTRAP):
    """Total lag of a consumer group on a topic: sum of (high watermark - committed)."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        tps = [TopicPartition(topic, p) for p in parts]
        total = 0
        for tp in c.committed(tps, timeout=10):
            _low, high = c.get_watermark_offsets(tp, timeout=10)
            total += high - max(tp.offset, 0)
        return total
    finally:
        c.close()


def read_all(topic, bootstrap=config.KAFKA_BOOTSTRAP):
    """Read every message currently in a topic and return the list of values."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"reader-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    values, at_eof = [], set()
    try:
        while len(at_eof) < len(parts):
            msg = c.poll(5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    at_eof.add(msg.partition())
                    continue
                raise RuntimeError(msg.error())
            values.append(msg.value())
    finally:
        c.close()
    return values
