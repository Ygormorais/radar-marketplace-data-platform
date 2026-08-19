"use client";

import { BarChart, FunnelChart, GaugeChart, LineChart, ScatterChart } from "echarts/charts";
import {
  AriaComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import type { EChartsOption } from "echarts";
import ReactECharts from "echarts-for-react/esm/core";
import { CanvasRenderer } from "echarts/renderers";
import type { CSSProperties } from "react";

echarts.use([
  BarChart,
  FunnelChart,
  GaugeChart,
  LineChart,
  ScatterChart,
  AriaComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  CanvasRenderer,
]);

export default function RadarChart({
  option,
  style,
  notMerge,
}: {
  option: unknown;
  style: CSSProperties;
  notMerge?: boolean;
}) {
  return (
    <ReactECharts
      echarts={echarts}
      option={option as EChartsOption}
      style={style}
      notMerge={notMerge}
    />
  );
}
