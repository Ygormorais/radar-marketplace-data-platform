"""Publica JSONL no Kafka/Redpanda preservando order_id como chave de partição."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _delivery_report(error: Any, message: Any) -> None:
    if error is not None:
        raise RuntimeError(f"Falha ao publicar evento: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="delivery-events-v1")
    parser.add_argument("--key-field", default="order_id")
    args = parser.parse_args()

    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise SystemExit("Instale o extra de streaming: pip install -e '.[streaming]'") from exc

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "radar-event-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
        }
    )
    published = 0
    with args.input.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            message_key = payload.get(args.key_field)
            if not isinstance(message_key, str) or not message_key:
                raise ValueError(f"{args.key_field} inválido na linha {line_number}")
            producer.produce(
                args.topic,
                key=message_key.encode(),
                value=line.strip().encode(),
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            published += 1
    remaining = producer.flush(30)
    if remaining:
        raise TimeoutError(f"{remaining} eventos não foram confirmados pelo broker")
    print(json.dumps({"published": published, "topic": args.topic}))


if __name__ == "__main__":
    main()
