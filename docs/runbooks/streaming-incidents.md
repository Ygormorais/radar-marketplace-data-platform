# Runbook — streaming e alertas operacionais

## SLOs

- heartbeat das queries: no máximo 20 minutos sem atualização;
- quarentena: até 2% dos eventos recebidos na janela de 24 horas;
- falhas de query e testes críticos de qualidade: tolerância zero;
- recuperação: RTO de 30 minutos e RPO limitado ao último checkpoint confirmado.

## Triagem

1. Localize o `run_id` no Monitoring Hub do Fabric e em `ctl_streaming_runs`.
2. Classifique o alerta em `ctl_operational_alerts`: heartbeat, falha da query, quarentena ou qualidade.
3. Inspecione o último `batch_id`, taxas de entrada/processamento e `error_message`.
4. Confirme conectividade com Event Hubs/Kafka e se o lag está aumentando.
5. Valide espaço, permissões e consistência dos diretórios em `Files/checkpoints/bronze`.

## Recuperação segura

- Reexecute `pl_streaming_supervisor` com o mesmo checkpoint; o merge por `event_id` evita duplicidade lógica.
- Não remova checkpoints durante um incidente comum. Isso altera o ponto de retomada e exige aprovação.
- Para payload incompatível, preserve a quarentena, corrija o produtor/contrato e faça replay controlado.
- Se um checkpoint estiver comprovadamente corrompido, mova-o para uma área de retenção, registre o incidente e reinicie com `starting_offsets` explicitamente definido.

## Entrega de alertas

`ctl_operational_alerts` funciona como outbox append-only. No Fabric, conecte a tabela a Data Activator ou a uma atividade Web/Teams. O consumidor deve atualizar `delivery_status` de `PENDING` para `DELIVERED` e usar `run_id + alert_code + observed_at` como chave idempotente.
