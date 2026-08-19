# ADR 0003 — Política para dados sintéticos

- Status: aceito
- Data: 2026-08-18

## Contexto

A base Olist é relacional e realista, porém pequena para benchmarking Spark e não possui telemetria logística contínua.

## Decisão

Gerar eventos determinísticos ligados a IDs reais quando disponíveis. Todo registro sintético deve carregar `synthetic=true` e `generator_seed`. Métricas de benchmark devem declarar perfil, seed e proporção sintética.

## Consequências

- escala e casos patológicos podem ser reproduzidos;
- resultados sintéticos não podem ser apresentados como fatos da Olist;
- alterações do gerador são mudanças de contrato e precisam de versionamento.

