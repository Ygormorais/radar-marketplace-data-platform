# ADR 0002 — Fabric Pipelines como orquestrador de autoridade

- Status: aceito
- Data: 2026-08-18

## Contexto

Fabric Data Factory é central ao projeto, enquanto Airflow é uma competência recorrente no mercado. Duplicar a DAG em ambos cria corridas, ownership ambíguo e troubleshooting ruim.

## Decisão

Fabric Pipeline manterá dependências, retries e parâmetros internos. O DAG Airflow futuro terá somente três responsabilidades: iniciar o pipeline mestre pela API, acompanhar o run e propagar seu estado.

## Consequências

- uma única fonte de verdade para a execução;
- Airflow continua demonstrado em uma integração realista;
- indisponibilidade do Airflow não corrompe a lógica interna do pipeline Fabric.

