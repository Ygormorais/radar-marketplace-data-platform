# ADR 0004 — SCD2 e versões atrasadas

- Status: aceito
- Data: 2026-08-18

## Contexto

Um snapshot de customer, seller ou produto pode chegar com data efetiva anterior à versão corrente. Expirar apenas a versão corrente produziria intervalos sobrepostos ou apagaria contexto histórico.

## Decisão

O merge SCD2 aceita somente novas chaves ou alterações posteriores a `valid_from` corrente. Alterações anteriores ou simultâneas são isoladas com `LATE_ARRIVING_SCD2_VERSION`. A correção exige um backfill ordenado de toda a chave natural.

O fechamento da versão corrente e a inserção da nova versão são executados em um único `MERGE` Delta por meio de staged rows com merge keys reais/nulas.

## Consequências

- não existem correções históricas silenciosas;
- a tabela preserva intervalos consistentes;
- o runbook de backfill precisa reconstruir uma chave completa quando um evento atrasado for aceito.

