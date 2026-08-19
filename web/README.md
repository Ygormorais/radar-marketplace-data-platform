# Radar — Marketplace Data Platform Web

Dashboard público e interativo do portfólio Radar. Ele não substitui o Power BI: consome um snapshot agregado e anonimizado das mesmas marts Gold para que recrutadores e avaliadores possam explorar o produto sem acesso ao tenant Fabric.

## Executar

```powershell
npm ci
npm run dev
```

O dashboard estará em `http://localhost:3000`.

## Validar

```powershell
npm run lint
npm test
```

O build usa vinext, Vite e o plugin Sites, produzindo saída ESM compatível com Cloudflare Workers.

## Dados

`public/data/dashboard.json` é um snapshot demonstrativo por padrão. Para substituí-lo por agregações reais da Gold exportada em Parquet:

```powershell
python ../scripts/export_web_marts.py `
  --gold-root C:/exports/radar `
  --output public/data/dashboard.json
```

O exportador publica somente agregações e converte a chave de seller em alias irreversível. Dados pessoais, mensagens de review e eventos de baixo nível não entram no artefato público.
