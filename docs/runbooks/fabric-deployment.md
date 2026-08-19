# Runbook — deployment Microsoft Fabric

## Escopo

O script `scripts/deploy_fabric.py` publica notebooks e pipelines a partir de `fabric/` usando a Fabric Items API. O processo é idempotente: itens são identificados por `(tipo, displayName)`, criados quando ausentes e atualizados quando existentes.

Ordem de publicação:

1. notebooks `*.Notebook`;
2. pipelines `*.DataPipeline`;
3. resolução dos tokens `{{workspace_id}}` e `{{item:Tipo:Nome}}` antes do envio.

Na execução, `nb_05_prepare_run` lê o manifesto imutável da landing, valida os nove arquivos e devolve o mapa de hashes SHA-256 ao `nb_10_bronze_batch`. O pipeline não aceita uma carga batch sem proveniência auditável.

O deploy falha fechado quando encontra referência ausente, item duplicado ou token não resolvido.

## Pré-requisitos do tenant

- workspace em capacidade Fabric;
- service principal habilitado para as APIs do Fabric;
- service principal com papel Contributor ou superior no workspace;
- federated credential do repositório GitHub configurada no Microsoft Entra ID;
- ambientes GitHub `dev`, `test` e `prod` com proteção de aprovação para produção.

Variáveis por GitHub Environment:

| Variável | Uso |
|---|---|
| `AZURE_CLIENT_ID` | application/client ID da identidade de deployment |
| `AZURE_TENANT_ID` | tenant do Fabric |
| `FABRIC_WORKSPACE_ID` | workspace específico do ambiente |

Não há client secret. O workflow usa OIDC e solicita um token de curta duração para `https://api.fabric.microsoft.com`.

## Dry-run local

```powershell
.\.venv\Scripts\python.exe scripts\deploy_fabric.py `
  --workspace-id 00000000-0000-0000-0000-000000000000 `
  --dry-run
```

O dry-run valida descoberta, ordem, Base64 e resolução de referências sem acessar o tenant.

## Deploy manual autenticado

Defina `FABRIC_WORKSPACE_ID` e um `FABRIC_TOKEN` temporário no processo atual e execute:

```powershell
.\.venv\Scripts\python.exe scripts\deploy_fabric.py
```

Nunca grave o token em `.env`, logs ou artefatos do workflow.

## Rollback

O script não exclui itens e não executa rollback destrutivo. Para reverter:

1. reverta o commit que alterou a definição;
2. execute novamente o workflow para o mesmo ambiente;
3. confirme a atualização dos itens e rode `pl_batch_master` com um conjunto de hashes conhecido.

## Diagnóstico

- `429`: o cliente respeita `Retry-After` e tenta novamente até cinco vezes;
- `202`: o cliente acompanha a Long Running Operation até estado terminal;
- referência não resolvida: confirme nome e sufixo do diretório do notebook;
- item duplicado: remova ou renomeie manualmente a duplicidade no workspace antes do deploy;
- quality gate falhou: não prossiga para serving; consulte `Tables/ctl_data_quality_results` pelo `run_id`.
