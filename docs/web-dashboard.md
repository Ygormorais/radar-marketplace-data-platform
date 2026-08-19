# Dashboard web público

## Propósito

O dashboard em `web/` torna o projeto demonstrável sem credenciais do Microsoft Fabric. A aplicação é uma superfície pública de portfólio; o Power BI Direct Lake continua sendo o serving corporativo.

```text
Fabric Warehouse Gold
        │
        ├── TMDL / Direct Lake ──► Power BI
        │
        └── export agregado ─────► dashboard.json ──► Radar Web
```

## Funcionalidades

- visão executiva com GMV, pedidos, ticket, SLA e avaliação;
- filtros por ano, UF e origem de tráfego;
- ranking e análise de risco de sellers;
- risco logístico por estado e priorização operacional;
- funil sessão → produto → carrinho → checkout → compra;
- painel de quality gates e lineage das marts;
- layout responsivo, navegação por teclado e suporte a redução de movimento;
- card Open Graph específico para compartilhamento no LinkedIn.

## Segurança do dado público

O exportador `scripts/export_web_marts.py` lê Parquet materializado da Gold com DuckDB e grava apenas agregações. Chaves de seller são transformadas em aliases SHA-256 com salt. Não são publicados customer IDs, CEP, coordenadas, texto de review, payloads brutos ou eventos individuais.

O arquivo versionado no repositório usa `metadata.mode=demo`. Uma exportação real usa `metadata.mode=gold-export`, evitando apresentar valores sintéticos como indicadores reais.

## Execução local

```powershell
cd web
npm ci
npm run dev
```

## Exportação das marts

```powershell
python -m pip install -e ".[web-export]"
python scripts/export_web_marts.py `
  --gold-root C:/exports/radar `
  --output web/public/data/dashboard.json
```

A raiz deve possuir os diretórios `gold/<modelo>/**/*.parquet` e `mart/<modelo>/**/*.parquet`.
