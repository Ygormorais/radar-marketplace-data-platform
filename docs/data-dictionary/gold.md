# Dicionário de dados — Gold

## Dimensões

| Modelo | Grão | Chave | Observação |
|---|---|---|---|
| `dim_date` | um dia | `date_key` | spine denso de 2016 a 2030 |
| `dim_customer` | uma versão do customer | `customer_key` | SCD2 por `customer_unique_id` |
| `dim_seller` | uma versão do seller | `seller_key` | SCD2, com cidade e UF históricas |
| `dim_product` | uma versão do produto | `product_key` | SCD2, preserva mudança de categoria e atributos |
| `dim_category` | uma categoria | `category_key` | membro desconhecido `-1` |
| `dim_geography` | um CEP prefix | `geography_key` | coordenada representativa e contagem da fonte |
| `dim_order_status` | um status canônico | `order_status_key` | ordenação explícita do lifecycle |
| `bridge_category_hierarchy` | par ancestral-descendente | chave composta | preparada para hierarquia variável |

## Fatos

| Modelo | Grão | Medidas principais |
|---|---|---|
| `fct_order_item` | item do pedido | item, frete, gross amount, lead time e atraso |
| `fct_payment` | parcela de pagamento | valor, parcelas e sequência |
| `fct_delivery` | pedido logístico | lead time, atraso, risco e transições inválidas |
| `fct_review` | review do pedido | nota e atraso associado |
| `fct_funnel_session` | sessão digital | flags view, cart, checkout e purchase |
| `fct_order_reconciliation` | pedido | diferença pedido-pagamento e status de reconciliação |

## Marts

| Modelo | Grão | Consumidor |
|---|---|---|
| `mart_executive_daily` | dia | KPIs e tendência executiva |
| `mart_seller_scorecard` | versão de seller | ranking, risco e operação comercial |
| `mart_delivery_sla` | dia, carrier e UF | gestão logística |
| `mart_funnel_conversion` | dia, device e origem | aquisição e conversão |
| `mart_customer_cohort` | coorte e meses desde primeira compra | retenção e LTV |

Todas as dimensões conformadas possuem membro desconhecido quando aplicável. PK/FK do Fabric Warehouse são declaradas `NOT ENFORCED`; integridade e unicidade são verificadas nos testes dbt.
