"use client";

import ReactECharts from "echarts-for-react/esm/core";
import * as echarts from "echarts/core";
import { BarChart, FunnelChart, GaugeChart, LineChart, ScatterChart } from "echarts/charts";
import { AriaComponent, GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { useEffect, useMemo, useState } from "react";

echarts.use([BarChart, FunnelChart, GaugeChart, LineChart, ScatterChart, AriaComponent, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

type Monthly = { month: string; gmv: number; orders: number; sla: number; review: number };
type Region = { state: string; gmv: number; orders: number; atRisk: number; late: number; sla: number };
type Seller = { id: string; state: string; gmv: number; orders: number; sla: number; avgDelay: number };
type Channel = { source: string; sessions: number; views: number; carts: number; checkouts: number; purchases: number };
type Previous = { gmv: number; orders: number; ticket: number; sla: number; review: number };
type Period = { year: number; previous: Previous; monthly: Monthly[]; regions: Region[]; sellers: Seller[]; channels: Channel[] };
type QualityCheck = { name: string; scope: string; status: "passed" | "warning"; coverage: number; failedRows: number };
type DashboardData = {
  metadata: { mode: string; generatedAt: string; label: string; sourceModels: string[] };
  periods: Period[];
  quality: QualityCheck[];
};

type Tab = "executive" | "logistics" | "funnel" | "quality";

const currency = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL", notation: "compact", maximumFractionDigits: 1 });
const integer = new Intl.NumberFormat("pt-BR");
const decimal = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const percent = new Intl.NumberFormat("pt-BR", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });

const chartText = { color: "#8ca4bb", fontFamily: "Arial" };
const gridLine = { lineStyle: { color: "#294259", opacity: 0.65 } };
const axisLine = { lineStyle: { color: "#36516a" } };
const tooltip = { trigger: "axis", backgroundColor: "#0b1b2c", borderColor: "#34536e", textStyle: { color: "#f4f8fc" } };

function delta(current: number, previous: number, suffix = "%") {
  const value = previous === 0 ? 0 : ((current - previous) / previous) * 100;
  return { label: `${value >= 0 ? "+" : ""}${decimal.format(value)}${suffix}`, positive: value >= 0 };
}

function KpiCard({ label, value, change, detail, tone = "teal" }: { label: string; value: string; change?: ReturnType<typeof delta>; detail: string; tone?: string }) {
  return (
    <article className={`kpi-card tone-${tone}`}>
      <div className="kpi-top"><span className="kpi-label">{label}</span><i aria-hidden="true" /></div>
      <strong>{value}</strong>
      <div className="kpi-foot">
        {change && <span className={change.positive ? "delta positive" : "delta negative"}>{change.positive ? "▲" : "▼"} {change.label}</span>}
        <span>{detail}</span>
      </div>
    </article>
  );
}

function ChartPanel({ kicker, title, action, children, className = "" }: { kicker: string; title: string; action?: React.ReactNode; children: React.ReactNode; className?: string }) {
  return (
    <article className={`panel ${className}`}>
      <div className="panel-heading">
        <div><span className="section-kicker">{kicker}</span><h2>{title}</h2></div>
        {action}
      </div>
      {children}
    </article>
  );
}

export default function Home() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loadError, setLoadError] = useState(false);
  const [tab, setTab] = useState<Tab>("executive");
  const [year, setYear] = useState(2018);
  const [region, setRegion] = useState("ALL");
  const [channel, setChannel] = useState("ALL");
  const [architectureOpen, setArchitectureOpen] = useState(false);

  useEffect(() => {
    fetch("/data/dashboard.json")
      .then((response) => {
        if (!response.ok) throw new Error("dataset unavailable");
        return response.json() as Promise<DashboardData>;
      })
      .then(setData)
      .catch(() => setLoadError(true));
  }, []);

  useEffect(() => {
    if (!architectureOpen) return;

    const previousOverflow = document.body.style.overflow;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setArchitectureOpen(false);
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", closeOnEscape);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [architectureOpen]);

  const period = useMemo(() => data?.periods.find((item) => item.year === year) ?? null, [data, year]);
  const regions = useMemo(() => period?.regions.filter((item) => region === "ALL" || item.state === region) ?? [], [period, region]);
  const sellers = useMemo(() => period?.sellers.filter((item) => region === "ALL" || item.state === region) ?? [], [period, region]);
  const channels = useMemo(() => period?.channels.filter((item) => channel === "ALL" || item.source === channel) ?? [], [period, channel]);

  if (loadError) {
    return <main className="state-page"><span className="brand-mark" /><h1>Não foi possível carregar o snapshot</h1><p>Confirme que o arquivo público de marts foi gerado antes do build.</p><button onClick={() => location.reload()}>Tentar novamente</button></main>;
  }
  if (!data || !period) {
    return <main className="state-page" aria-live="polite"><span className="loading-ring" /><h1>Preparando indicadores</h1><p>Carregando o contrato analítico da camada Gold.</p></main>;
  }

  const totals = regions.reduce((acc, item) => ({ gmv: acc.gmv + item.gmv, orders: acc.orders + item.orders, late: acc.late + item.late, atRisk: acc.atRisk + item.atRisk, slaWeighted: acc.slaWeighted + item.sla * item.orders }), { gmv: 0, orders: 0, late: 0, atRisk: 0, slaWeighted: 0 });
  const allGmv = period.monthly.reduce((sum, item) => sum + item.gmv, 0);
  const allOrders = period.monthly.reduce((sum, item) => sum + item.orders, 0);
  const selectedGmv = region === "ALL" ? allGmv : totals.gmv;
  const selectedOrders = region === "ALL" ? allOrders : totals.orders;
  const selectedSla = region === "ALL" ? period.monthly.reduce((sum, item) => sum + item.sla * item.orders, 0) / allOrders : totals.slaWeighted / totals.orders;
  const review = period.monthly.reduce((sum, item) => sum + item.review * item.orders, 0) / allOrders;
  const ticketValue = selectedGmv / selectedOrders;
  const regionFactor = selectedGmv / allGmv;

  const gmvOption = {
    aria: { enabled: true, description: `Evolução mensal de GMV em ${year}` },
    animationDuration: 550,
    color: ["#2dd4bf", "#28a9f7"],
    tooltip: { ...tooltip, valueFormatter: (value: number) => currency.format(value) },
    grid: { left: 58, right: 22, top: 28, bottom: 42 },
    xAxis: { type: "category", data: period.monthly.map((item) => item.month), axisLabel: chartText, axisLine },
    yAxis: { type: "value", axisLabel: { ...chartText, formatter: (value: number) => `${(value / 1000000).toFixed(1)} mi` }, splitLine: gridLine },
    series: [{ name: "GMV", type: "line", smooth: 0.35, symbolSize: 7, lineStyle: { width: 3 }, areaStyle: { opacity: 0.14 }, data: period.monthly.map((item) => Math.round(item.gmv * regionFactor)) }],
  };

  const regionOption = {
    aria: { enabled: true, description: "Pedidos em risco e atrasados por estado" },
    color: ["#ffbd66", "#ff7189"], tooltip, legend: { data: ["Em risco", "Atrasados"], textStyle: chartText, top: 0, right: 0 },
    grid: { left: 48, right: 18, top: 38, bottom: 38 },
    xAxis: { type: "category", data: regions.map((item) => item.state), axisLabel: chartText, axisLine },
    yAxis: { type: "value", axisLabel: chartText, splitLine: gridLine },
    series: [{ name: "Em risco", type: "bar", barMaxWidth: 24, itemStyle: { borderRadius: [5, 5, 0, 0] }, data: regions.map((item) => item.atRisk) }, { name: "Atrasados", type: "bar", barMaxWidth: 24, itemStyle: { borderRadius: [5, 5, 0, 0] }, data: regions.map((item) => item.late) }],
  };

  const sellerOption = {
    aria: { enabled: true, description: "Ranking dos sellers por GMV" }, color: ["#28a9f7"], tooltip: { ...tooltip, valueFormatter: (value: number) => currency.format(value) },
    grid: { left: 86, right: 28, top: 8, bottom: 24 },
    xAxis: { type: "value", axisLabel: { ...chartText, formatter: (value: number) => currency.format(value) }, splitLine: gridLine },
    yAxis: { type: "category", inverse: true, data: sellers.slice(0, 6).map((item) => item.id), axisLabel: chartText, axisLine },
    series: [{ type: "bar", barWidth: 15, itemStyle: { borderRadius: [0, 8, 8, 0] }, data: sellers.slice(0, 6).map((item) => item.gmv) }],
  };

  const sellerScatterOption = {
    aria: { enabled: true, description: "Relação entre atraso médio, GMV e volume de pedidos por seller" }, color: ["#2dd4bf"],
    tooltip: { trigger: "item", backgroundColor: "#0b1b2c", borderColor: "#34536e", textStyle: { color: "#f4f8fc" }, formatter: (params: { data: { name: string; value: number[] } }) => `${params.data.name}<br/>Atraso: ${params.data.value[0]} dias<br/>GMV: ${currency.format(params.data.value[1])}<br/>Pedidos: ${integer.format(params.data.value[2])}` },
    grid: { left: 56, right: 24, top: 20, bottom: 44 },
    xAxis: { type: "value", name: "atraso médio (dias)", nameTextStyle: chartText, axisLabel: chartText, axisLine, splitLine: gridLine },
    yAxis: { type: "value", axisLabel: { ...chartText, formatter: (value: number) => currency.format(value) }, splitLine: gridLine },
    series: [{ type: "scatter", data: sellers.map((item) => ({ name: item.id, value: [item.avgDelay, item.gmv, item.orders], symbolSize: Math.max(14, Math.sqrt(item.orders) / 2) })) }],
  };

  const aggregateChannel = channels.reduce((acc, item) => ({ sessions: acc.sessions + item.sessions, views: acc.views + item.views, carts: acc.carts + item.carts, checkouts: acc.checkouts + item.checkouts, purchases: acc.purchases + item.purchases }), { sessions: 0, views: 0, carts: 0, checkouts: 0, purchases: 0 });
  const funnelOption = {
    aria: { enabled: true, description: "Funil entre sessões e compras" }, color: ["#28a9f7", "#29b9da", "#2bc7c6", "#2dd4bf", "#78e3d2"],
    tooltip: { ...tooltip, trigger: "item", valueFormatter: (value: number) => integer.format(value) },
    series: [{ type: "funnel", left: "6%", width: "88%", top: 10, bottom: 8, minSize: "18%", maxSize: "100%", sort: "descending", gap: 4, label: { color: "#f4f8fc", formatter: "{b}  {c}" }, itemStyle: { borderColor: "#10243a", borderWidth: 2 }, data: [{ value: aggregateChannel.sessions, name: "Sessões" }, { value: aggregateChannel.views, name: "Visualização" }, { value: aggregateChannel.carts, name: "Carrinho" }, { value: aggregateChannel.checkouts, name: "Checkout" }, { value: aggregateChannel.purchases, name: "Compra" }] }],
  };

  const channelOption = {
    aria: { enabled: true, description: "Conversão final por origem de tráfego" }, color: ["#2dd4bf"], tooltip: { ...tooltip, valueFormatter: (value: number) => percent.format(value) },
    grid: { left: 52, right: 18, top: 22, bottom: 42 }, xAxis: { type: "category", data: channels.map((item) => item.source), axisLabel: chartText, axisLine },
    yAxis: { type: "value", axisLabel: { ...chartText, formatter: (value: number) => `${(value * 100).toFixed(0)}%` }, splitLine: gridLine },
    series: [{ type: "bar", barMaxWidth: 44, itemStyle: { borderRadius: [7, 7, 0, 0] }, data: channels.map((item) => item.purchases / item.sessions) }],
  };

  const qualityScore = data.quality.reduce((sum, item) => sum + item.coverage, 0) / data.quality.length;
  const qualityOption = {
    aria: { enabled: true, description: `Índice de qualidade ${percent.format(qualityScore)}` },
    series: [{ type: "gauge", startAngle: 210, endAngle: -30, min: 0.95, max: 1, splitNumber: 5, progress: { show: true, width: 18, itemStyle: { color: "#2dd4bf" } }, axisLine: { lineStyle: { width: 18, color: [[1, "#203b52"]] } }, pointer: { show: false }, axisTick: { show: false }, splitLine: { show: false }, axisLabel: { show: false }, anchor: { show: false }, title: { offsetCenter: [0, "42%"], color: "#8ca4bb", fontSize: 11 }, detail: { valueAnimation: true, offsetCenter: [0, "0%"], color: "#f4f8fc", fontSize: 34, fontWeight: 700, formatter: (value: number) => `${(value * 100).toFixed(2)}%` }, data: [{ value: qualityScore, name: "quality gate" }] }],
  };

  const tabs: { id: Tab; label: string }[] = [{ id: "executive", label: "Executivo" }, { id: "logistics", label: "Logística" }, { id: "funnel", label: "Funil digital" }, { id: "quality", label: "Qualidade" }];

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div className="brand-block"><span className="brand-mark" aria-hidden="true" /><div><p className="eyebrow">RADAR / MARKETPLACE DATA PLATFORM</p><h1>{tabs.find((item) => item.id === tab)?.label}</h1></div></div>
        <div className="header-actions"><button className="ghost-button" onClick={() => setArchitectureOpen(true)}>Ver arquitetura</button><span className="status-pill"><i /> Snapshot Gold</span></div>
      </header>

      <div className="tab-list" aria-label="Áreas do dashboard" role="tablist">
        {tabs.map((item) => <button key={item.id} role="tab" aria-selected={tab === item.id} className={tab === item.id ? "active" : ""} onClick={() => setTab(item.id)}>{item.label}</button>)}
      </div>

      <section className="filter-bar" aria-label="Filtros analíticos">
        <label><span>Ano</span><select value={year} onChange={(event) => setYear(Number(event.target.value))}>{data.periods.map((item) => <option key={item.year}>{item.year}</option>)}</select></label>
        <label><span>UF</span><select value={region} onChange={(event) => setRegion(event.target.value)}><option value="ALL">Brasil</option>{period.regions.map((item) => <option key={item.state}>{item.state}</option>)}</select></label>
        <label><span>Origem</span><select value={channel} onChange={(event) => setChannel(event.target.value)}><option value="ALL">Todos os canais</option>{period.channels.map((item) => <option key={item.source}>{item.source}</option>)}</select></label>
        <button className="reset-button" onClick={() => { setRegion("ALL"); setChannel("ALL"); }}>Limpar filtros</button>
        <div className="data-caption"><span>{data.metadata.label}</span></div>
      </section>

      {tab === "executive" && <>
        <section className="kpi-grid" aria-label="Indicadores executivos">
          <KpiCard label="GMV" value={currency.format(selectedGmv)} change={region === "ALL" ? delta(selectedGmv, period.previous.gmv) : undefined} detail={region === "ALL" ? "vs. período anterior" : `recorte ${region}`} />
          <KpiCard label="Pedidos" value={integer.format(selectedOrders)} change={region === "ALL" ? delta(selectedOrders, period.previous.orders) : undefined} detail={region === "ALL" ? "vs. período anterior" : `recorte ${region}`} tone="blue" />
          <KpiCard label="Ticket médio" value={currency.format(ticketValue)} change={region === "ALL" ? delta(ticketValue, period.previous.ticket) : undefined} detail="por pedido" tone="violet" />
          <KpiCard label="Entrega no prazo" value={percent.format(selectedSla)} change={region === "ALL" ? delta(selectedSla, period.previous.sla, "%") : undefined} detail="SLA realizado" tone="amber" />
          <KpiCard label="Avaliação média" value={`${decimal.format(review)} / 5`} change={region === "ALL" ? delta(review, period.previous.review) : undefined} detail="reviews respondidos" tone="pink" />
        </section>
        <section className="analytics-grid executive-grid">
          <ChartPanel kicker="PERFORMANCE" title="GMV por mês" className="wide"><ReactECharts echarts={echarts} option={gmvOption} style={{ height: 315 }} notMerge /></ChartPanel>
          <ChartPanel kicker="SELLER SCORECARD" title="Ranking por GMV"><ReactECharts echarts={echarts} option={sellerOption} style={{ height: 315 }} notMerge /></ChartPanel>
          <ChartPanel kicker="RISCO OPERACIONAL" title="Risco logístico por UF" className="half"><ReactECharts echarts={echarts} option={regionOption} style={{ height: 285 }} notMerge /></ChartPanel>
          <ChartPanel kicker="DECISÃO" title="Leitura executiva" className="half insight-panel">
            <div className="insight-callout"><b>01</b><div><strong>SLA pressionado</strong><p>RJ, BA e PE concentram a maior diferença para a meta operacional de 90%.</p></div></div>
            <div className="insight-callout"><b>02</b><div><strong>Crescimento sustentável</strong><p>GMV cresce acima do volume, indicando expansão do ticket médio.</p></div></div>
            <div className="insight-callout"><b>03</b><div><strong>Próxima ação</strong><p>Priorizar sellers de alto GMV com atraso médio positivo no playbook logístico.</p></div></div>
          </ChartPanel>
        </section>
      </>}

      {tab === "logistics" && <>
        <section className="kpi-grid compact-kpis">
          <KpiCard label="SLA realizado" value={percent.format(selectedSla)} detail="meta: 90%" tone="amber" />
          <KpiCard label="Pedidos em risco" value={integer.format(totals.atRisk)} detail="snapshot operacional" tone="amber" />
          <KpiCard label="Pedidos atrasados" value={integer.format(totals.late)} detail={`${percent.format(totals.late / selectedOrders)} do volume`} tone="pink" />
          <KpiCard label="Sellers monitorados" value={integer.format(sellers.length)} detail="scorecard priorizado" tone="blue" />
        </section>
        <section className="analytics-grid logistics-grid">
          <ChartPanel kicker="GEOGRAFIA" title="Exposição por UF" className="half"><ReactECharts echarts={echarts} option={regionOption} style={{ height: 330 }} notMerge /></ChartPanel>
          <ChartPanel kicker="SELLER RISK" title="Atraso médio × GMV" className="half"><ReactECharts echarts={echarts} option={sellerScatterOption} style={{ height: 330 }} notMerge /></ChartPanel>
          <ChartPanel kicker="PRIORIZAÇÃO" title="Sellers para intervenção" className="full table-panel">
            <div className="table-wrap"><table><thead><tr><th>Seller</th><th>UF</th><th>GMV</th><th>Pedidos</th><th>SLA</th><th>Atraso médio</th><th>Classificação</th></tr></thead><tbody>{[...sellers].sort((a, b) => a.sla - b.sla).map((item) => <tr key={item.id}><td data-label="Seller"><code>{item.id}</code></td><td data-label="UF">{item.state}</td><td data-label="GMV">{currency.format(item.gmv)}</td><td data-label="Pedidos">{integer.format(item.orders)}</td><td data-label="SLA">{percent.format(item.sla)}</td><td data-label="Atraso médio">{decimal.format(item.avgDelay)} d</td><td data-label="Classificação"><span className={`badge ${item.sla < .86 ? "critical" : item.sla < .9 ? "warning" : "healthy"}`}>{item.sla < .86 ? "Crítico" : item.sla < .9 ? "Atenção" : "Saudável"}</span></td></tr>)}</tbody></table></div>
          </ChartPanel>
        </section>
      </>}

      {tab === "funnel" && <>
        <section className="kpi-grid compact-kpis">
          <KpiCard label="Sessões" value={integer.format(aggregateChannel.sessions)} detail={channel === "ALL" ? "todos os canais" : channel} tone="blue" />
          <KpiCard label="Carrinhos" value={integer.format(aggregateChannel.carts)} detail={`${percent.format(aggregateChannel.carts / aggregateChannel.sessions)} das sessões`} tone="violet" />
          <KpiCard label="Compras" value={integer.format(aggregateChannel.purchases)} detail="sessões convertidas" tone="teal" />
          <KpiCard label="Conversão" value={percent.format(aggregateChannel.purchases / aggregateChannel.sessions)} detail="sessão → compra" tone="pink" />
        </section>
        <section className="analytics-grid funnel-grid">
          <ChartPanel kicker="JORNADA" title="Funil consolidado" className="half"><ReactECharts echarts={echarts} option={funnelOption} style={{ height: 370 }} notMerge /></ChartPanel>
          <ChartPanel kicker="AQUISIÇÃO" title="Conversão por origem" className="half"><ReactECharts echarts={echarts} option={channelOption} style={{ height: 370 }} notMerge /></ChartPanel>
          <ChartPanel kicker="DIAGNÓSTICO" title="Eficiência entre etapas" className="full stage-grid-panel">
            <div className="stage-grid">{[
              ["Sessão → produto", aggregateChannel.views / aggregateChannel.sessions], ["Produto → carrinho", aggregateChannel.carts / aggregateChannel.views], ["Carrinho → checkout", aggregateChannel.checkouts / aggregateChannel.carts], ["Checkout → compra", aggregateChannel.purchases / aggregateChannel.checkouts],
            ].map(([label, value]) => <div className="stage-card" key={String(label)}><span>{label}</span><strong>{percent.format(Number(value))}</strong><i><b style={{ width: `${Number(value) * 100}%` }} /></i></div>)}</div>
          </ChartPanel>
        </section>
      </>}

      {tab === "quality" && <section className="analytics-grid quality-grid">
        <ChartPanel kicker="QUALITY GATE" title="Confiabilidade do snapshot" className="quality-gauge"><ReactECharts echarts={echarts} option={qualityOption} style={{ height: 300 }} notMerge /></ChartPanel>
        <ChartPanel kicker="CONTRATOS" title="Resultado dos testes" className="quality-table table-panel">
          <div className="table-wrap"><table><thead><tr><th>Teste</th><th>Escopo</th><th>Cobertura</th><th>Falhas</th><th>Status</th></tr></thead><tbody>{data.quality.map((item) => <tr key={item.name}><td data-label="Teste">{item.name}</td><td data-label="Escopo">{item.scope}</td><td data-label="Cobertura">{percent.format(item.coverage)}</td><td data-label="Falhas">{integer.format(item.failedRows)}</td><td data-label="Status"><span className={`badge ${item.status === "passed" ? "healthy" : "warning"}`}>{item.status === "passed" ? "Aprovado" : "Alerta"}</span></td></tr>)}</tbody></table></div>
        </ChartPanel>
        <ChartPanel kicker="LINEAGE" title="Marts que alimentam esta aplicação" className="full lineage-panel"><div className="lineage-flow"><span>OneLake<br/><b>Silver Delta</b></span><i>→</i><span>Fabric Warehouse<br/><b>dbt Gold</b></span><i>→</i>{data.metadata.sourceModels.map((model) => <span key={model}><small>MART</small><b>{model.replace("mart_", "")}</b></span>)}<i>→</i><span>Snapshot público<br/><b>JSON versionado</b></span></div></ChartPanel>
      </section>}

      <footer><span>Dados demonstrativos — nenhuma métrica é apresentada como resultado real da Olist.</span><span>Gerado em {new Date(data.metadata.generatedAt).toLocaleString("pt-BR")}</span></footer>

      {architectureOpen && <div className="drawer-backdrop"><button className="drawer-dismiss-layer" aria-label="Fechar painel de arquitetura" onClick={() => setArchitectureOpen(false)} /><aside className="architecture-drawer" role="dialog" aria-modal="true" aria-labelledby="architecture-title"><button className="drawer-close" aria-label="Fechar" onClick={() => setArchitectureOpen(false)}>×</button><span className="section-kicker">ARQUITETURA DA PLATAFORMA</span><h2 id="architecture-title">Do evento ao indicador</h2><p>O dashboard é uma superfície pública e desacoplada. O serving corporativo continua sendo Power BI Direct Lake.</p><ol><li><b>01</b><div><strong>Ingestão</strong><span>Olist batch, API incremental e eventos logísticos.</span></div></li><li><b>02</b><div><strong>Bronze + Silver</strong><span>Spark, Delta, CDC, SCD2 e quality gates.</span></div></li><li><b>03</b><div><strong>Gold</strong><span>Fabric Warehouse, dbt, fatos e dimensões conformadas.</span></div></li><li><b>04</b><div><strong>Serving</strong><span>Power BI Direct Lake e snapshot anonimizado para a web.</span></div></li></ol><div className="drawer-note">O arquivo público contém somente agregações; IDs completos e eventos de baixo nível não são publicados.</div></aside></div>}
    </main>
  );
}
