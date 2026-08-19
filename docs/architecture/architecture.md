# Arquitetura técnica

## Princípios

1. **Reprocessável:** landing imutável, manifestos e execução identificada por `run_id`.
2. **Idempotente:** retries não podem duplicar o estado lógico.
3. **Contratos antes de transformação:** schema drift inesperado segue para quarantine.
4. **Separação de compute e storage:** Delta/OneLake é o contrato entre workloads.
5. **Código testável:** notebooks Fabric apenas parametrizam e chamam módulos Python.
6. **Observabilidade como dado:** execução, SLOs, qualidade e alertas são persistidos e analisáveis.
7. **Portabilidade pragmática:** execução local reproduz sem simular que é o Fabric.

## Fluxo de dados

| Camada | Responsabilidade | Mutabilidade | Tecnologia principal |
|---|---|---:|---|
| Landing | Evidência exata recebida da fonte | Imutável | OneLake Files |
| Bronze | Envelope técnico e payload bruto | Append-only | Spark + Delta |
| Silver | Entidades conformadas e reconciliadas | MERGE controlado | Spark + Delta |
| Gold | Modelo dimensional e métricas oficiais | Incremental | Fabric Warehouse + dbt |
| Serving | Modelo semântico e relatórios | Versionada | Power BI Direct Lake |

## Ambientes

- `local`: arquivos locais, Redpanda e testes sem credenciais cloud;
- `dev`: workspace Fabric de desenvolvimento e dados reduzidos;
- `test`: workspace isolado, testes de integração e promoção;
- `prod`: workspace produtivo, identidade gerenciada e schedules ativos.

Configurações possuem o mesmo schema. IDs de workspace, conexões e segredos serão resolvidos no deployment, nunca gravados no código.

## Orquestração e deployment

`pl_batch_master` é a autoridade sobre a DAG batch. A etapa `Prepare Run` valida o manifesto imutável e entrega os hashes SHA-256 ao Bronze; a Silver comercial antecede logística e reconciliação, que executam em paralelo. Após ambas concluírem, a ordem é estrita: quality gate, `dbt build --fail-fast` e refresh transacional do modelo semântico.

`pl_streaming_supervisor` executa delivery e clickstream em paralelo. O modo padrão `availableNow` cria microbatches finitos adequados ao agendamento do Fabric sem perder checkpoints; `continuous` permanece disponível para compute dedicado. Cada query grava heartbeat, throughput, batch e erro em `ctl_streaming_runs`; `nb_40_operational_health` avalia SLOs e escreve alertas idempotentes em `ctl_operational_alerts`.

O deployment segue trunk-based development com workspaces separados por ambiente. O GitHub Actions autentica por OIDC e usa a Fabric Items API para criar ou atualizar notebooks e pipelines. IDs físicos não são versionados: referências como `{{item:Notebook:nb_10_bronze_batch}}` são resolvidas contra o catálogo do workspace imediatamente antes do envio. Produção usa GitHub Environment com aprovação obrigatória.

## Serving analítico

A Silver é exposta ao Warehouse dentro do workspace. O dbt materializa dimensões, fatos e marts em schemas separados; o modelo semântico TMDL usa Direct Lake sobre essas tabelas e mantém as medidas de negócio em DAX. O relatório PBIR referencia o modelo por caminho no desenvolvimento e é rebinding por ambiente na promoção.

O relatório PBIR possui páginas de visão executiva, operação logística, funil digital e performance de sellers. O dashboard web publica apenas agregados anonimizados e funciona como superfície demonstrável sem credenciais do tenant.

O gerador local continua servindo a contratos e cenários funcionais. Benchmarks de volume usam o gerador Spark distribuído e registram parâmetros e métricas na auditoria, evitando apresentar execução single-process como evidência de escala.
