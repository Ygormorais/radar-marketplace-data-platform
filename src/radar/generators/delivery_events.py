"""Geração determinística de eventos logísticos para testes e streaming."""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from radar.contracts.events import DeliveryEvent, DeliveryStatus

DEFAULT_STATES = ("SP", "RJ", "MG", "PR", "RS", "SC", "BA", "PE", "GO", "DF")
DEFAULT_CARRIERS = ("carrier_sul", "carrier_sudeste", "carrier_nacional")
HAPPY_PATH = (
    DeliveryStatus.CREATED,
    DeliveryStatus.APPROVED,
    DeliveryStatus.INVOICED,
    DeliveryStatus.SHIPPED,
    DeliveryStatus.IN_TRANSIT,
    DeliveryStatus.OUT_FOR_DELIVERY,
    DeliveryStatus.DELIVERED,
)


def read_order_ids(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if "order_id" not in (reader.fieldnames or []):
            raise ValueError(f"Coluna order_id ausente em {path}")
        order_ids = [row["order_id"].strip() for row in reader if row.get("order_id", "").strip()]
    if not order_ids:
        raise ValueError(f"Nenhum order_id válido em {path}")
    return order_ids


def synthetic_order_ids(count: int) -> list[str]:
    if count <= 0:
        raise ValueError("count deve ser positivo")
    return [f"synthetic_order_{index:012d}" for index in range(1, count + 1)]


def generate_delivery_events(
    order_ids: Sequence[str],
    *,
    seed: int,
    base_time: datetime,
    duplicate_rate: float = 0.0,
    late_event_rate: float = 0.0,
) -> Iterator[DeliveryEvent]:
    """Gera ciclos de entrega; a mesma entrada sempre produz a mesma saída."""

    if base_time.tzinfo is None or base_time.utcoffset() is None:
        raise ValueError("base_time deve conter timezone")
    if not 0 <= duplicate_rate < 1 or not 0 <= late_event_rate < 1:
        raise ValueError("taxas devem estar no intervalo [0, 1)")

    # PRNG intencional: reprodutibilidade, não geração de segredo.
    rng = random.Random(seed)  # noqa: S311
    for order_index, order_id in enumerate(order_ids):
        state = rng.choice(DEFAULT_STATES)
        carrier = rng.choice(DEFAULT_CARRIERS)
        order_start = base_time + timedelta(minutes=order_index * 3)
        elapsed_hours = 0
        generated: list[DeliveryEvent] = []
        for sequence, status in enumerate(HAPPY_PATH, start=1):
            elapsed_hours += rng.randint(1, 36)
            occurred_at = order_start + timedelta(hours=elapsed_hours)
            produced_delay = rng.randint(0, 20)
            if rng.random() < late_event_rate:
                produced_delay += rng.randint(180, 1440)
            event_key = f"{order_id}:{sequence}:{status.value}"
            event = DeliveryEvent(
                event_id=uuid5(NAMESPACE_URL, f"radar:{event_key}"),
                order_id=order_id,
                status=status,
                occurred_at=occurred_at,
                produced_at=occurred_at + timedelta(minutes=produced_delay),
                location_state=state,
                carrier_code=carrier,
                sequence_number=sequence,
                attributes={"synthetic": True, "generator_seed": seed},
            )
            generated.append(event)
            yield event
            if rng.random() < duplicate_rate:
                yield event.model_copy(deep=True)

        # Reordena apenas o envio, não o timestamp de negócio, simulando arrival fora de ordem.
        if len(generated) > 3 and rng.random() < late_event_rate:
            yield generated[-2].model_copy(
                update={"event_id": uuid5(NAMESPACE_URL, f"late:{order_id}")}
            )


def write_jsonl(events: Iterable[DeliveryEvent], target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with target.open("w", encoding="utf-8", newline="\n") as stream:
        for event in events:
            stream.write(event.model_dump_json() + "\n")
            count += 1
    return count


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--orders-csv", type=Path)
    source.add_argument("--order-count", type=int, default=1000)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260818)
    parser.add_argument("--base-time", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--late-event-rate", type=float, default=0.03)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    order_ids = (
        read_order_ids(args.orders_csv)
        if args.orders_csv
        else synthetic_order_ids(args.order_count)
    )
    events = generate_delivery_events(
        order_ids,
        seed=args.seed,
        base_time=datetime.fromisoformat(args.base_time).astimezone(UTC),
        duplicate_rate=args.duplicate_rate,
        late_event_rate=args.late_event_rate,
    )
    count = write_jsonl(events, args.output)
    print(json.dumps({"event_count": count, "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
