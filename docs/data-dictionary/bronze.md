# Dicionário de dados — Bronze

## Convenções técnicas batch

Todas as tabelas `bronze_<source>` preservam as colunas da fonte e adicionam:

| Coluna | Tipo | Descrição |
|---|---|---|
| `_record_hash` | string | SHA-256 determinístico das colunas de negócio, com representação explícita de nulo |
| `_source_file` | string | Caminho físico observado pelo Spark |
| `_source_file_hash` | string | SHA-256 do arquivo registrado no manifesto de landing |
| `_run_id` | string | Execução responsável pela primeira inserção |
| `_ingested_at` | timestamp | Horário UTC da ingestão |
| `_ingestion_date` | date | Partição física Bronze |
| `_source_name` | string | Identificador canônico da fonte |
| `_corrupt_record` | string | Payload capturado pelo parser CSV quando malformado |

Identidade física: `(_source_file_hash, _record_hash)`.

## Fontes Olist

| Fonte | Grão | Chave natural |
|---|---|---|
| customers | cliente por pedido | `customer_id` |
| geolocation | coordenada por prefixo CEP | prefixo, latitude, longitude |
| order_items | item do pedido | `order_id, order_item_id` |
| payments | parcela/meio de pagamento | `order_id, payment_sequential` |
| reviews | avaliação do pedido | `review_id, order_id` |
| orders | pedido | `order_id` |
| products | produto | `product_id` |
| sellers | seller | `seller_id` |
| category_translation | tradução da categoria | `product_category_name` |

Os nomes incorretos `*_lenght` são preservados na Bronze por fidelidade à fonte e corrigidos somente na Silver.

## Eventos logísticos

`bronze_delivery_events` possui o contrato publicado em `contracts/events/delivery_event.v1.schema.json` e metadados Kafka:

- `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`;
- `_micro_batch_id`;
- `_ingested_at`, `_ingestion_date`.

A chave idempotente é `event_id`. `occurred_at` é event time; `_kafka_timestamp` e `_ingested_at` não substituem o tempo do negócio.

`bronze_clickstream_events` segue `contracts/events/clickstream_event.v1.schema.json`, usa `user_id` como chave Kafka e `event_id` para idempotência. Eventos desconhecidos recebem `INVALID_EVENT_TYPE`.

## Quarantine

Reason codes atuais:

- `MALFORMED_RECORD`;
- `MISSING_REQUIRED_FIELD`;
- `DUPLICATE_IN_SOURCE_FILE`;
- `INVALID_JSON`;
- `UNSUPPORTED_SCHEMA_VERSION`;
- `INVALID_STATUS`;
- `INVALID_STATE`.
