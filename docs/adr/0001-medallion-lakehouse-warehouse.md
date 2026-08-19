# ADR 0001 — Lakehouse nas camadas operacionais e Warehouse na Gold

- Status: aceito
- Data: 2026-08-18

## Contexto

Bronze/Silver precisam processar volume, JSON, CDC, eventos fora de ordem e transformações distribuídas. A Gold precisa de modelagem dimensional, T-SQL, dbt e consumo Power BI.

## Decisão

Usar Lakehouses Delta para Bronze/Silver e Fabric Warehouse para Gold. O Warehouse lerá a Silver por integração OneLake/cross-database no mesmo workspace. dbt será responsável pela DAG SQL da Gold.

## Consequências

- cada engine executa o workload para o qual é mais adequada;
- o modelo dimensional ganha uma superfície T-SQL governada;
- há mais de um item Fabric para implantar e observar;
- constraints do Warehouse não são enforced, então integridade será validada no pipeline e no dbt.

