# Power BI e modelo semântico

O projeto versionado está em `powerbi/Radar.pbip`. Ele contém um modelo semântico TMDL e um relatório PBIR; portanto, tabelas, relacionamentos, medidas DAX, páginas e visuais produzem diffs revisáveis em pull requests.

![Prévia da visão executiva](assets/dashboard-preview.svg)

> A imagem usa valores ilustrativos. O PBIR real consulta as marts Gold pelo modelo Direct Lake.

## Páginas e contratos

| Bloco | Origem | Métrica ou dimensão |
|---|---|---|
| KPIs executivos | `mart.mart_executive_daily` | GMV, pedidos, ticket médio, SLA e avaliação |
| Tendência de GMV | `mart.mart_executive_daily` + `gold.dim_date` | GMV por dia e período selecionado |
| Ranking de sellers | `mart.mart_seller_scorecard` + `gold.dim_seller` | GMV vendedor |
| Risco logístico | `mart.mart_delivery_sla` | pedidos em risco e atrasados por UF |
| Conversão | `mart.mart_funnel_conversion` | sessão para compra por origem de tráfego |

- **Visão executiva:** GMV, pedidos, ticket, SLA, avaliação, sellers, risco e conversão.
- **Operação logística:** risco/atraso por UF, atraso por transportadora e tendência diária.
- **Funil digital:** sessões, compras, conversão, origem, dispositivo e evolução temporal.
- **Performance de sellers:** GMV, pedidos, SLA, ranking individual e distribuição por UF.

## Abrir no Power BI Desktop

1. Execute a Gold no Warehouse com `dbt build`.
2. Em `powerbi/Radar.SemanticModel/definition/expressions.tmdl`, substitua o host placeholder e `wh_gold` pelos valores do Warehouse do ambiente.
3. Abra `powerbi/Radar.pbip` no Power BI Desktop com PBIP/PBIR habilitado.
4. Faça login no tenant, confirme o binding do Warehouse e valide o modo Direct Lake.
5. Publique o modelo e o relatório no mesmo workspace; no deployment, substitua conexão e IDs por ambiente.

O host está deliberadamente sem credencial. Autenticação é resolvida pela identidade do usuário no Desktop e por service principal ou workload identity na automação.

## Validação sem abrir o Desktop

```powershell
npm ci
npx --no-install powerbi-report-author validate `
  powerbi/Radar.pbip --format text
```

Essa validação cobre estrutura PBIP/PBIR, schemas JSON, páginas, visuais, referências e metadados de plataforma. A validação de consultas DAX contra dados reais ocorre após o binding ao Warehouse no ambiente Fabric.

## Evoluções opcionais

- RLS por região e carteira de sellers;
- calculation group para período anterior, YoY e YTD;
- deployment rules para conexão `dev`, `test` e `prod`.
