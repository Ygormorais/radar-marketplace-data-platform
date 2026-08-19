# Runbook — publicação Gold e modelo semântico

## Pré-requisitos por workspace

1. Anexe aos notebooks um Fabric Environment contendo o wheel `radar`, `dbt-core`, `dbt-fabric` e o ODBC Driver 18.
2. Sincronize `dbt/` para `Files/dbt` no Lakehouse padrão e materialize `profiles.yml` a partir de `profiles.example.yml`.
3. Configure identidade gerenciada ou service principal com acesso ao Warehouse; não grave secrets no projeto dbt.
4. Publique o modelo semântico `Radar` e crie uma conexão Power BI autorizada para refresh.
5. Cadastre `FABRIC_POWERBI_CONNECTION_ID` como variável protegida do GitHub Environment.

## Ordem transacional

`Quality Gate → dbt build --fail-fast → refresh semântico`.

O refresh não é iniciado se testes Spark ou dbt falharem. O `commitMode=transactional` evita expor um modelo parcialmente atualizado, e o pipeline aguarda a conclusão do refresh antes de encerrar.

## Rollback

- Para regressão SQL, reverta o commit e execute novamente o pipeline; modelos dbt são reconstruídos conforme a materialização declarada.
- Para incidente de dados, restaure as tabelas Delta pelo histórico/time travel antes de republicar Gold.
- Não force refresh semântico enquanto o quality gate estiver vermelho.

## Limite conhecido

A atividade nativa de dbt Job do Fabric ainda está em preview. O Radar usa um notebook executor versionado para manter a definição REST do pipeline validável e o comportamento testável. Quando o contrato REST dessa atividade estiver estável, a substituição afeta apenas o estágio `Build Gold with dbt`.
