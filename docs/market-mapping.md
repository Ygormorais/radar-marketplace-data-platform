# Mapeamento com vagas de engenharia de dados

Leitura atualizada em 18/08/2026 de vagas sêniores representativas. Não é uma contagem estatística; é uma matriz de aderência baseada em requisitos recorrentes observados em anúncios como [RAVL](https://jobs.lever.co/ravl_io/9e942ef6-d1c4-4404-84b7-de6cd6c94b21), [WatchGuard](https://jobs.lever.co/watchguard/4172f749-beab-43ea-9ad4-1d5c4d333665), [SteerBridge](https://jobs.lever.co/steerbridge/084800cd-1bae-4b31-b653-da05c521b2d6) e [Breakwater](https://jobs.lever.co/BreakwaterTech/45372c18-b24d-4a36-b05a-06e616b08450).

| Competência recorrente | Evidência no Radar |
|---|---|
| Python/PySpark e processamento distribuído | pacote `src/radar`, notebooks finos, schemas explícitos, gerador distribuído e testes Spark |
| SQL avançado e tuning | windows, CTEs aninhadas/recursivas, gaps-and-islands, constraints e decisões de particionamento |
| Lakehouse/medallion/Delta | Landing imutável, Bronze append-only, Silver conformada e Gold dimensional |
| Modelagem dimensional | dimensões SCD2, fatos, bridge hierárquica, marts e star schema |
| dbt/DataOps | staging→dimensions→facts→marts, testes genéricos/singulares, lineage e fail-fast |
| Orquestração | Fabric Data Factory com DAGs batch e streaming, parâmetros, retries, timeouts e dependências |
| Batch, incremental, CDC e streaming | manifesto/hash, MERGE idempotente, current state, watermark, late events e checkpoints |
| Kafka/event-driven | Redpanda local compatível com Kafka, dois contratos versionados e produtores idempotentes |
| Qualidade, contratos e observabilidade | JSON Schema/Pydantic, quarantine, RI, quality gate, SLOs, audit e alert outbox |
| Cloud Azure/Fabric | OneLake, Lakehouse, Warehouse, Data Factory, Notebooks e Power BI Direct Lake |
| CI/CD e Git | workflows de PR, integração, deployment OIDC dev/test/prod e Fabric Items API |
| BI e camada semântica | TMDL, DAX, PBIR versionável, quatro páginas e refresh transacional |
| Segurança/governança | RBAC SQL, identidade sem secrets no código, anonimização do export público e runbooks |
| Documentação/ownership sênior | ADRs, dicionário, runbooks, SLO/RTO/RPO e decisões com trade-offs explícitos |

## O que não foi duplicado deliberadamente

- **Airflow/Prefect:** aparecem em muitas vagas, mas duplicar a mesma DAG criaria dois control planes. O Fabric Data Factory é o orquestrador autoritativo; o ADR 0002 documenta como portar as dependências.
- **Kubernetes:** adequado para operar plataformas e serviços de longa duração, mas não acrescenta valor real ao workload serverless gerenciado do Fabric. Redpanda local usa Compose apenas como ambiente reproduzível.
- **Uma segunda cloud/warehouse:** AWS, GCP, Snowflake e Databricks são alternativas de plataforma, não requisitos que devam coexistir artificialmente no mesmo produto.
- **Machine learning/MLOps:** não existe caso de negócio validado que justifique um modelo. Incluir um notebook preditivo decorativo diluiria a evidência de engenharia de dados.
- **Terraform para itens Fabric:** notebooks e pipelines usam a Items API oficial e OIDC. IaC seria indicado para recursos Azure periféricos em um tenant real; IDs, capacidade e políticas organizacionais não devem ser fictícios no repositório.
