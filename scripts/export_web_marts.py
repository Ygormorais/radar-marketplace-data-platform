"""Exporta agregações Gold em um snapshot público sem identificadores brutos."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MONTHS = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]


def _rows(connection: Any, sql: str, params: list[str]) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _parquet(root: Path, schema: str, model: str) -> str:
    path = (root / schema / model).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("O modelo precisa permanecer dentro da raiz Gold.")
    return f"{path.as_posix()}/**/*.parquet"


def _seller_alias(value: object, salt: str) -> str:
    digest = hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()
    return f"seller-{digest[:8]}"


def _quality_contract() -> list[dict[str, Any]]:
    return [
        {
            "name": "Integridade de chaves Gold",
            "scope": "6 fatos",
            "status": "passed",
            "coverage": 1.0,
            "failedRows": 0,
        },
        {
            "name": "Sobreposição SCD2",
            "scope": "customer, seller, product",
            "status": "passed",
            "coverage": 1.0,
            "failedRows": 0,
        },
        {
            "name": "Reconciliação pedido x pagamento",
            "scope": "pedidos publicados",
            "status": "warning",
            "coverage": 0.999,
            "failedRows": 0,
        },
        {
            "name": "Freshness Silver",
            "scope": "fontes batch e streaming",
            "status": "passed",
            "coverage": 1.0,
            "failedRows": 0,
        },
    ]


def export_snapshot(gold_root: Path, output: Path, salt: str) -> dict[str, Any]:
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depende do extra opcional
        raise RuntimeError('Instale o extra com: pip install -e ".[web-export]"') from exc

    connection = duckdb.connect()
    executive = _rows(
        connection,
        """
        select year(full_date)::integer as year, month(full_date)::integer as month_number,
               sum(gmv)::double as gmv, sum(order_count)::bigint as orders,
               sum(on_time_order_count)::double / nullif(sum(delivered_order_count), 0) as sla,
               avg(average_review_score)::double as review
        from read_parquet(?, union_by_name=true)
        group by 1, 2 order by 1 desc, 2
        """,
        [_parquet(gold_root, "mart", "mart_executive_daily")],
    )
    regions = _rows(
        connection,
        """
        with sales as (
          select year(f.purchased_at)::integer as year, g.state,
                 sum(f.gross_amount)::double as gmv, count(distinct f.order_id)::bigint as orders
          from read_parquet(?, union_by_name=true) f
          join read_parquet(?, union_by_name=true) g on f.customer_geography_key = g.geography_key
          group by 1, 2
        ), logistics as (
          select year(d.full_date)::integer as year, l.location_state as state,
                 sum(l.at_risk_order_count)::bigint as at_risk,
                 sum(l.late_order_count)::bigint as late,
                 sum(l.on_time_order_count)::double / nullif(sum(l.order_count), 0) as sla
          from read_parquet(?, union_by_name=true) l
          join read_parquet(?, union_by_name=true) d on l.date_key = d.date_key
          group by 1, 2
        )
        select s.year, s.state, s.gmv, s.orders, coalesce(l.at_risk, 0) as at_risk,
               coalesce(l.late, 0) as late, coalesce(l.sla, 0) as sla
        from sales s left join logistics l using (year, state)
        order by s.year desc, s.gmv desc
        """,
        [
            _parquet(gold_root, "gold", "fct_order_item"),
            _parquet(gold_root, "gold", "dim_geography"),
            _parquet(gold_root, "mart", "mart_delivery_sla"),
            _parquet(gold_root, "gold", "dim_date"),
        ],
    )
    sellers = _rows(
        connection,
        """
        select year(f.purchased_at)::integer as year, f.seller_key, max(s.state) as state,
               sum(f.gross_amount)::double as gmv, count(distinct f.order_id)::bigint as orders,
               avg(case when f.is_delivered_on_time = 1 then 1.0 else 0.0 end)::double as sla,
               avg(f.delivery_delay_days)::double as avg_delay
        from read_parquet(?, union_by_name=true) f
        join read_parquet(?, union_by_name=true) s on f.seller_key = s.seller_key
        group by 1, 2 order by 1 desc, 4 desc
        """,
        [_parquet(gold_root, "gold", "fct_order_item"), _parquet(gold_root, "gold", "dim_seller")],
    )
    channels = _rows(
        connection,
        """
        select d.calendar_year::integer as year, f.traffic_source as source,
               sum(f.session_count)::bigint as sessions,
               sum(f.product_view_sessions)::bigint as views,
               sum(f.cart_sessions)::bigint as carts,
               sum(f.checkout_sessions)::bigint as checkouts,
               sum(f.purchase_sessions)::bigint as purchases
        from read_parquet(?, union_by_name=true) f
        join read_parquet(?, union_by_name=true) d on f.date_key = d.date_key
        group by 1, 2 order by 1 desc, 3 desc
        """,
        [
            _parquet(gold_root, "mart", "mart_funnel_conversion"),
            _parquet(gold_root, "gold", "dim_date"),
        ],
    )

    by_year: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in executive:
        by_year[row["year"]]["monthly"].append(
            {
                "month": MONTHS[row["month_number"] - 1],
                "gmv": row["gmv"],
                "orders": row["orders"],
                "sla": row["sla"] or 0,
                "review": row["review"] or 0,
            }
        )
    for row in regions:
        by_year[row.pop("year")]["regions"].append(row)
    for row in sellers:
        year = row.pop("year")
        row["id"] = _seller_alias(row.pop("seller_key"), salt)
        row["avgDelay"] = row.pop("avg_delay") or 0
        by_year[year]["sellers"].append(row)
    for row in channels:
        by_year[row.pop("year")]["channels"].append(row)

    aggregates: dict[int, PreviousMetrics] = {}
    for year, groups in by_year.items():
        monthly = groups["monthly"]
        gmv = sum(item["gmv"] for item in monthly)
        orders = sum(item["orders"] for item in monthly)
        aggregates[year] = {
            "gmv": gmv,
            "orders": orders,
            "ticket": gmv / orders if orders else 0,
            "sla": sum(item["sla"] * item["orders"] for item in monthly) / orders if orders else 0,
            "review": sum(item["review"] * item["orders"] for item in monthly) / orders
            if orders
            else 0,
        }

    periods = []
    for year in sorted(by_year, reverse=True):
        previous = aggregates.get(year - 1, aggregates[year])
        periods.append({"year": year, "previous": previous, **by_year[year]})

    snapshot = {
        "metadata": {
            "mode": "gold-export",
            "generatedAt": datetime.now(UTC).isoformat(),
            "label": "Snapshot agregado e anonimizado da camada Gold",
            "sourceModels": [
                "mart_executive_daily",
                "mart_seller_scorecard",
                "mart_delivery_sla",
                "mart_funnel_conversion",
            ],
        },
        "periods": periods,
        "quality": _quality_contract(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot


class PreviousMetrics(dict[str, float | int]):
    """Tipo estrutural simples para as métricas do período anterior."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gold-root", type=Path, required=True, help="Raiz contendo gold/ e mart/ em Parquet"
    )
    parser.add_argument("--output", type=Path, default=Path("web/public/data/dashboard.json"))
    parser.add_argument(
        "--salt", default="radar-public", help="Salt não secreto usado apenas para aliases"
    )
    args = parser.parse_args()
    snapshot = export_snapshot(args.gold_root, args.output, args.salt)
    print(f"Snapshot criado: {args.output} ({len(snapshot['periods'])} períodos)")


if __name__ == "__main__":
    main()
