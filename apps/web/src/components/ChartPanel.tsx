import { VegaEmbed } from 'react-vega';
import type { VisualizationSpec } from 'vega-embed';

import type { ChartSpec, QueryPreview } from '../types';

interface ChartPanelProps {
  chart: ChartSpec | null;
  preview: QueryPreview | null;
}

export function ChartPanel({ chart, preview }: ChartPanelProps) {
  if (!chart || !preview || preview.row_count === 0) {
    return null;
  }

  if (chart.renderer === 'summary') {
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

  if (chart.renderer === 'table') {
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

  const spec = withData(chart.spec, preview);

  return (
    <section className="overflow-hidden rounded-2xl border border-neutral-800 bg-neutral-950/70 p-3">
      <VegaEmbed
        options={{ actions: false, renderer: 'svg' }}
        spec={spec}
        style={{ width: '100%' }}
      />
    </section>
  );
}

function withData(spec: Record<string, unknown>, preview: QueryPreview): VisualizationSpec {
  return {
    ...(spec as unknown as VisualizationSpec),
    data: {
      values: rowsToObjects(preview),
    },
  } as VisualizationSpec;
}

function rowsToObjects(preview: QueryPreview) {
  return preview.rows.map((row) =>
    Object.fromEntries(preview.columns.map((column, index) => [column, row[index]])),
  );
}

function prettify(value: string) {
  return value.replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, (part) => part.toUpperCase());
}

function formatValue(value: unknown) {
  return typeof value === 'number'
    ? value.toLocaleString(undefined, { maximumFractionDigits: 2 })
    : value == null
      ? 'Null'
      : String(value);
}
