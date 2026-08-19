# Runbook — reprocessamento Bronze

1. Identifique `run_id`, fonte, hash e erro em `ctl_ingestion_audit`.
2. Confirme que o arquivo no landing possui o mesmo SHA-256 do manifesto.
3. Não remova checkpoints nem tabelas para corrigir falhas de regra.
4. Execute novamente com um novo `run_id` e o mesmo hash.
5. Confirme `rows_written=0` quando o lote já havia sido concluído.
6. Para corrigir contrato, publique nova versão, reingira em novo destino e reconcilie contagens.

## Streaming

- Reinício comum: preserve `Files/checkpoints/bronze` e reinicie a mesma query.
- Mudança incompatível de query/state: use novo checkpoint e novo nome de tabela ou faça migração explícita.
- `failOnDataLoss=true` é intencional; perda de offsets deve abrir incidente, não ser ignorada.
- Eventos posteriores ao watermark podem não participar da deduplicação stateful, mas o `MERGE` por `event_id` continua impedindo duplicação física.

