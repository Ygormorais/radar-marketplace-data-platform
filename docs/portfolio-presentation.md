# Apresentação do portfólio

## Checklist de publicação

- [ ] Substituir autor/contato no `README` e habilitar Issues no GitHub.
- [ ] Proteger `main`, exigir CI e configurar environments `dev`, `test` e `prod`.
- [ ] Adicionar secrets/variables OIDC e executar primeiro deployment no workspace `dev`.
- [ ] Anexar o Fabric Environment com o wheel Radar e dependências dbt aos notebooks.
- [ ] Executar batch, streaming supervisor e capturar o Monitoring Hub com um `run_id` comum.
- [ ] Abrir `Radar.pbip`, fazer binding ao Warehouse e publicar modelo/relatório.
- [ ] Atualizar o snapshot web com uma exportação Gold anonimizada ou manter o modo claramente marcado como demo.
- [ ] Publicar o dashboard web e inserir o link no About do repositório e no LinkedIn.

## Capturas recomendadas

1. arquitetura completa e fronteiras de responsabilidade;
2. DAG batch mostrando o gate antes de dbt/refresh;
3. supervisor de streaming e `ctl_streaming_runs`;
4. quality gate com uma falha controlada e posterior recuperação;
5. lineage dbt e star schema no modelo semântico;
6. as quatro páginas do Power BI;
7. dashboard web em desktop e mobile;
8. CI verde e deployment por ambiente.

## Roteiro de vídeo — 3 a 4 minutos

**0:00–0:30 — problema:** risco logístico, reconciliação e funil de marketplace; por que batch e eventos coexistem.

**0:30–1:10 — arquitetura:** Landing/Bronze/Silver/Gold, contratos, fronteira Spark/dbt e escolha do Fabric como control plane.

**1:10–2:00 — execução:** manifesto e idempotência batch; watermark, late events, checkpoint e modo `availableNow`; telemetria por `run_id`.

**2:00–2:40 — confiança:** quarantine, RI, SCD2, reconciliação, quality gate, SLOs e por que o refresh não ocorre após falha.

**2:40–3:20 — consumo:** marts, DAX, Power BI e dashboard público anonimizado.

**3:20–3:40 — engenharia:** testes, CI/CD OIDC, ADRs, runbooks e trade-offs deliberados (sem DAG Airflow duplicada e sem ML decorativo).

## Texto curto para LinkedIn

> Radar é uma plataforma de dados de marketplace construída em Microsoft Fabric com PySpark, Delta, dbt, SQL e Power BI. O projeto cobre ingestão batch e streaming, SCD2/CDC, reconciliação financeira, quality gates, observabilidade com SLOs, modelo dimensional, CI/CD por OIDC e uma superfície web pública com dados agregados. O foco não foi acumular ferramentas, mas demonstrar ownership de produção, contratos e decisões arquiteturais verificáveis.
