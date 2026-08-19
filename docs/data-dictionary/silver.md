# Dicionário de dados — Silver

## Entidades conformadas

| Tabela | Grão | Política de atualização |
|---|---|---|
| `silver_customers` | `customer_id` da origem | CDC current-state |
| `silver_customers_history` | versão de `customer_unique_id` | SCD tipo 2 |
| `silver_sellers_history` | versão de seller | SCD tipo 2 |
| `silver_products_history` | versão de produto | SCD tipo 2 |
| `silver_orders` | pedido | CDC por `_ingested_at` |
| `silver_order_items` | item do pedido | CDC por `_ingested_at` |
| `silver_payments` | pagamento sequencial | CDC por `_ingested_at` |
| `silver_reviews` | avaliação do pedido | CDC por `_ingested_at` |
| `silver_geography` | prefixo de CEP | current-state |
| `silver_financial_reconciliation` | pedido | recalculada incrementalmente |
| `silver_delivery_event_history` | evento logístico | append lógico por `event_id` |
| `silver_delivery_snapshot` | pedido | accumulating snapshot |
| `silver_clickstream_sessions` | sessão calculada | recomputável por event time |

## Colunas derivadas de pedidos

- `approval_lead_hours`: horas entre compra e aprovação;
- `delivery_lead_days`: dias fracionários entre compra e entrega;
- `delivery_delay_days`: diferença civil entre entrega e data prometida;
- `is_delivered_on_time`: nulo enquanto não entregue;
- `is_terminal_failure`: cancelado ou indisponível.

## Reconciliação financeira

O valor esperado é `sum(item_amount + freight_amount)` no grão do pedido. O valor realizado é `sum(payment_amount)`. A diferença é `realizado - esperado`.

Status possíveis:

- `RECONCILED`;
- `OVERPAID`;
- `UNDERPAID`;
- `ORDER_WITHOUT_ITEMS`;
- `ORDER_WITHOUT_PAYMENT`.

A tolerância padrão é R$ 0,01 e os cálculos usam `decimal(18,2)`, não `double`.

## SCD tipo 2

Colunas técnicas:

- `attribute_hash`;
- `valid_from`, inclusivo;
- `valid_to`, exclusivo;
- `is_current`;
- `created_run_id`, `updated_run_id`.

Uma versão com `effective_at <= valid_from` da versão corrente não é aplicada automaticamente. Ela recebe `LATE_ARRIVING_SCD2_VERSION`, pois corrigir intervalos históricos exige reconstruir as versões subsequentes.

## Sessionização

Uma nova sessão começa quando o intervalo desde o evento anterior do usuário é superior a 30 minutos. Ordenação: `occurred_at, event_id`. O ID é um SHA-256 determinístico de `user_id:session_sequence`.

Como um late event pode alterar limites e IDs das sessões seguintes, o notebook atual recompõe integralmente a tabela Silver em uma escrita Delta atômica. Um `MERGE` apenas por `session_id` deixaria sessões antigas órfãs. A otimização futura será recompor partições de usuários/janelas afetadas, mantendo a mesma semântica.

Flags de funil:

- `has_product_view`;
- `has_add_to_cart`;
- `has_checkout`;
- `has_purchase`.
