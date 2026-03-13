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
  const xAxisTitle = axisTitle(chart.x, preview);
  const yAxisTitle = yAxisTitleFor(chart.y, preview);

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
            title: {
              text: xAxisTitle,
              font: { color: '#a3a3a3', size: 12 },
            },
            gridcolor: 'rgba(64,64,64,0.45)',
            zerolinecolor: 'rgba(64,64,64,0.45)',
            linecolor: 'rgba(82,82,82,0.7)',
          },
          yaxis: {
            title: {
              text: yAxisTitle,
              font: { color: '#a3a3a3', size: 12 },
            },
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
  const palette = [
    '#60a5fa',
    '#f59e0b',
    '#34d399',
    '#f472b6',
    '#a78bfa',
    '#f87171',
    '#22d3ee',
    '#84cc16',
  ];

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

function axisTitle(key: string | null, preview: QueryPreview) {
  if (!key) {
    return '';
  }
  const unit = inferUnit(key);
  const isDatetime = isTimeishKey(key) || columnValuesLookLikeDatetimes(preview, key);
  if (isDatetime) {
    return `${prettify(key)} (UTC)`;
  }
  return unit ? `${prettify(key)} (${unit})` : prettify(key);
}

function yAxisTitleFor(keys: string[], preview: QueryPreview) {
  if (keys.length === 0) {
    return '';
  }
  const units = Array.from(
    new Set(keys.map(inferUnit).filter((value): value is string => value !== null)),
  );
  if (units.length === 1) {
    return units[0];
  }
  if (keys.length === 1) {
    return axisTitle(keys[0], preview);
  }
  return 'Value';
}

function inferUnit(key: string): string | null {
  const lowered = key.toLowerCase();
  if (
    lowered.includes('rrp') ||
    lowered.includes('rop') ||
    lowered.includes('eep') ||
    lowered.includes('price') ||
    lowered.includes('marginalvalue')
  ) {
    return 'AUD/MWh';
  }
  if (lowered.includes('demand') || lowered.includes('mw') || lowered.includes('flow')) {
    return 'MW';
  }
  if (lowered.includes('mwh') || lowered.includes('energy')) {
    return 'MWh';
  }
  if (lowered.includes('percent') || lowered.endsWith('pct')) {
    return '%';
  }
  return null;
}

function isTimeishKey(key: string) {
  const lowered = key.toLowerCase();
  return (
    lowered.includes('date') ||
    lowered.includes('time') ||
    lowered.includes('interval')
  );
}

function columnValuesLookLikeDatetimes(preview: QueryPreview, column: string) {
  const index = preview.columns.indexOf(column);
  if (index < 0) {
    return false;
  }
  return preview.rows
    .slice(0, 5)
    .every((row) => {
      const value = row[index];
      return typeof value === 'string' && !Number.isNaN(Date.parse(value));
    });
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
