"""Gerador local determinístico de sessões e eventos de funil."""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from radar.contracts.events import ClickstreamEvent, ClickstreamEventType


def generate_clickstream_events(
    *, user_count: int, seed: int, base_time: datetime
) -> Iterator[ClickstreamEvent]:
    if user_count <= 0:
        raise ValueError("user_count deve ser positivo")
    if base_time.tzinfo is None or base_time.utcoffset() is None:
        raise ValueError("base_time deve conter timezone")
    rng = random.Random(seed)  # noqa: S311 - geração reproduzível, não criptográfica
    for index in range(1, user_count + 1):
        user_id = f"synthetic_user_{index:010d}"
        product_id = f"synthetic_product_{rng.randint(1, 10_000):08d}"
        session_id = f"source_session_{index:012d}"
        conversion = rng.random()
        funnel = [ClickstreamEventType.PAGE_VIEW, ClickstreamEventType.PRODUCT_VIEW]
        if conversion < 0.70:
            funnel.append(ClickstreamEventType.ADD_TO_CART)
        if conversion < 0.50:
            funnel.append(ClickstreamEventType.CHECKOUT)
        if conversion < 0.40:
            funnel.append(ClickstreamEventType.PURCHASE)
        for sequence, event_type in enumerate(funnel, start=1):
            event_key = f"{user_id}:{session_id}:{sequence}:{event_type.value}"
            yield ClickstreamEvent(
                event_id=uuid5(NAMESPACE_URL, f"radar:{event_key}"),
                user_id=user_id,
                anonymous_id=f"anon_{index:010d}",
                session_source_id=session_id,
                event_type=event_type,
                occurred_at=base_time + timedelta(minutes=index * 2 + sequence),
                product_id=product_id if event_type != ClickstreamEventType.PAGE_VIEW else None,
                order_id=f"synthetic_order_{index:012d}"
                if event_type == ClickstreamEventType.PURCHASE
                else None,
                device_type=("mobile", "desktop", "tablet")[index % 3],
                traffic_source=("organic", "paid_search", "direct", "email")[index % 4],
            )


def write_clickstream_jsonl(events: Iterator[ClickstreamEvent], target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with target.open("w", encoding="utf-8", newline="\n") as stream:
        for event in events:
            stream.write(event.model_dump_json() + "\n")
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20260818)
    parser.add_argument("--base-time", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    events = generate_clickstream_events(
        user_count=args.user_count,
        seed=args.seed,
        base_time=datetime.fromisoformat(args.base_time).astimezone(UTC),
    )
    count = write_clickstream_jsonl(events, args.output)
    print(json.dumps({"event_count": count, "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
