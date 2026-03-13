import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-basic-dist-min';

import type { ChartSpec, QueryPreview } from '../types';

const Plot = createPlotlyComponent(Plotly);

interface ChartPanelProps {
  chart: ChartSpec | null;
  preview: QueryPreview | null;
}

export function ChartPanel({ chart, preview }: ChartPanelProps) {
  if (!chart || !preview || preview.row_count === 0) {
    return null;
  }

  if (chart.kind === 'summary') {
    const pairs = preview.columns.map((column, index) => ({
      label: prettify(column),
      value: formatValue(preview.rows[0]?.[index]),
    }));
    return (
      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {pairs.map((pair) => (
          <div
            key={pair.label}
            className="rounded-2xl border border-neutral-800 bg-neutral-950/70 p-4"
          >
            <div className="text-xs uppercase tracking-[0.18em] text-neutral-500">
              {pair.label}
            </div>
            <div className="mt-2 text-2xl font-semibold text-neutral-100">{pair.value}</div>
          </div>
        ))}
      </section>
    );
  }

  if (chart.kind === 'table' || !chart.x || chart.y.length === 0) {
    return (
      <section className="overflow-hidden rounded-2xl border border-neutral-800 bg-neutral-950/70">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-neutral-800 text-sm">
            <thead className="bg-neutral-900/80 text-neutral-400">
              <tr>
                {preview.columns.map((column) => (
                  <th key={column} className="px-4 py-3 text-left font-medium">
                    {prettify(column)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-900">
              {preview.rows.slice(0, 12).map((row, rowIndex) => (
                <tr key={rowIndex} className="bg-neutral-950/50">
                  {row.map((value, cellIndex) => (
                    <td key={`${rowIndex}-${cellIndex}`} className="px-4 py-3 text-neutral-200">
                      {formatValue(value)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    );
  }

  const frame = rowsToObjects(preview);
  const traces = buildTraces(frame, chart);

  return (
    <section className="overflow-hidden rounded-2xl border border-neutral-800 bg-neutral-950/70 p-3">
      <Plot
        data={traces as never}
        layout={{
          title: {
            text: chart.title,
            font: { family: 'Space Grotesk, sans-serif', color: '#f5f5f5', size: 16 },
            x: 0.02,
          },
          autosize: true,
          height: 360,
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          margin: { t: 56, r: 24, b: 44, l: 52 },
          font: { family: 'Space Grotesk, sans-serif', color: '#d4d4d4' },
          xaxis: {
            gridcolor: 'rgba(64,64,64,0.45)',
            zerolinecolor: 'rgba(64,64,64,0.45)',
            linecolor: 'rgba(82,82,82,0.7)',
          },
          yaxis: {
            gridcolor: 'rgba(64,64,64,0.45)',
            zerolinecolor: 'rgba(64,64,64,0.45)',
            linecolor: 'rgba(82,82,82,0.7)',
          },
          legend: {
            orientation: 'h',
            x: 0,
            y: 1.12,
            font: { color: '#a3a3a3' },
          },
        }}
        config={{
          responsive: true,
          displaylogo: false,
          modeBarButtonsToRemove: ['lasso2d', 'select2d'],
        }}
        style={{ width: '100%', height: '100%' }}
      />
    </section>
  );
}

function rowsToObjects(preview: QueryPreview) {
  return preview.rows.map((row) =>
    Object.fromEntries(preview.columns.map((column, index) => [column, row[index]])),
  );
}

function buildTraces(frame: Array<Record<string, unknown>>, chart: ChartSpec) {
  const xKey = chart.x!;
  const yKeys = chart.y;
  const colorKey = chart.color;
  const palette = ['#fafafa', '#d4d4d4', '#a3a3a3', '#737373'];

  if (!colorKey) {
    return yKeys.map((yKey, index) => ({
      type: chart.kind === 'bar' ? 'bar' : 'scatter',
      mode: chart.kind === 'line' ? 'lines+markers' : undefined,
      x: frame.map((row) => row[xKey]),
      y: frame.map((row) => row[yKey]),
      name: prettify(yKey),
      marker: { color: palette[index % palette.length] },
      line: { color: palette[index % palette.length], width: 2.5 },
    }));
  }

  const groups = new Map<string, Array<Record<string, unknown>>>();
  for (const row of frame) {
    const group = String(row[colorKey] ?? 'Other');
    if (!groups.has(group)) {
      groups.set(group, []);
    }
    groups.get(group)!.push(row);
  }

  return Array.from(groups.entries()).flatMap(([group, rows], groupIndex) =>
    yKeys.map((yKey, yIndex) => ({
      type: chart.kind === 'bar' ? 'bar' : 'scatter',
      mode: chart.kind === 'line' ? 'lines+markers' : undefined,
      x: rows.map((row) => row[xKey]),
      y: rows.map((row) => row[yKey]),
      name: yKeys.length > 1 ? `${group} · ${prettify(yKey)}` : group,
      marker: { color: palette[(groupIndex + yIndex) % palette.length] },
      line: { color: palette[(groupIndex + yIndex) % palette.length], width: 2.5 },
    })),
  );
}

function prettify(value: string) {
  return value
    .replaceAll('_', ' ')
    .toLowerCase()
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatValue(value: unknown) {
  if (typeof value === 'number') {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (value === null || value === undefined) {
    return 'Null';
  }
  return String(value);
}
