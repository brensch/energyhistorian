import { Suspense, lazy } from 'react';

import type {
  AssistantMessageMetadata,
  ChartSpec,
  FinalAnswer,
  MessageRecord,
  Plan,
  QueryPreview,
} from '../types';

const ChartPanel = lazy(async () => {
  const module = await import('./ChartPanel');
  return { default: module.ChartPanel };
});

export interface LiveRunState {
  phases: Array<{ phase: string; message: string }>;
  plan: Plan | null;
  sql: string | null;
  chart: ChartSpec | null;
  preview: QueryPreview | null;
  answer: FinalAnswer | null;
  error: string | null;
  busy: boolean;
}

interface MessageThreadProps {
  messages: MessageRecord[];
  liveRun: LiveRunState | null;
}

export function MessageThread({ messages, liveRun }: MessageThreadProps) {
  return (
    <section className="mx-auto flex w-full max-w-4xl flex-1 flex-col gap-6 px-4 py-8 sm:px-6">
      {messages.length === 0 ? <WelcomeCard /> : null}

      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
      ))}

      {liveRun ? <LiveRunBubble liveRun={liveRun} /> : null}
    </section>
  );
}

function WelcomeCard() {
  return (
    <article className="rounded-[2rem] border border-neutral-800 bg-neutral-900/70 p-8 shadow-[0_30px_90px_rgba(0,0,0,0.35)]">
      <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
        NEM Explorer
      </div>
      <h1 className="mt-3 text-4xl font-semibold tracking-tight text-neutral-50 sm:text-5xl">
        Ask the market a hard question.
      </h1>
      <p className="mt-4 max-w-2xl text-sm leading-7 text-neutral-400 sm:text-base">
        This chat plans SQL over the semantic warehouse, runs it against ClickHouse, and returns
        the answer together with the chart inside the thread.
      </p>
    </article>
  );
}

function MessageBubble({ message }: { message: MessageRecord }) {
  const metadata = message.metadata as AssistantMessageMetadata;
  const isUser = message.role === 'user';

  return (
    <article className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[88%] rounded-[1.75rem] border px-5 py-4 shadow-[0_20px_60px_rgba(0,0,0,0.25)] sm:max-w-[82%] ${
          isUser
            ? 'border-neutral-700 bg-neutral-100 text-neutral-950'
            : 'border-neutral-800 bg-neutral-900/90 text-neutral-100'
        }`}
      >
        <div
          className={`text-[11px] font-medium uppercase tracking-[0.18em] ${
            isUser ? 'text-neutral-500' : 'text-neutral-500'
          }`}
        >
          {isUser ? 'You' : 'NEM Explorer'}
        </div>
        <div className="mt-3 whitespace-pre-wrap text-sm leading-7 sm:text-[15px]">
          {message.content}
        </div>

        {!isUser && metadata.note ? (
          <p className="mt-3 text-xs leading-6 text-neutral-400">{metadata.note}</p>
        ) : null}

        {!isUser && (metadata.chart || metadata.preview) ? (
          <div className="mt-4">
            <Suspense
              fallback={
                <div className="rounded-2xl border border-neutral-800 bg-neutral-950/70 p-4 text-sm text-neutral-500">
                  Loading chart…
                </div>
              }
            >
              <ChartPanel chart={metadata.chart ?? null} preview={metadata.preview ?? null} />
            </Suspense>
          </div>
        ) : null}

        {message.sql_text ? (
          <details className="mt-4 rounded-2xl border border-neutral-800 bg-neutral-950/70 p-3">
            <summary className="cursor-pointer text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
              SQL
            </summary>
            <pre className="mt-3 overflow-x-auto whitespace-pre-wrap font-mono text-xs leading-6 text-neutral-300">
              {message.sql_text}
            </pre>
          </details>
        ) : null}
      </div>
    </article>
  );
}

function LiveRunBubble({ liveRun }: { liveRun: LiveRunState }) {
  return (
    <article className="flex justify-start">
      <div className="max-w-[88%] rounded-[1.75rem] border border-neutral-800 bg-neutral-900/90 px-5 py-4 text-neutral-100 shadow-[0_20px_60px_rgba(0,0,0,0.25)] sm:max-w-[82%]">
        <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-neutral-500">
          NEM Explorer
        </div>

        <div className="mt-3 flex flex-wrap gap-2">
          {liveRun.phases.map((phase, index) => (
            <div
              key={`${phase.phase}-${index}`}
              className="rounded-full border border-neutral-800 bg-neutral-950 px-3 py-1.5 text-xs text-neutral-400"
            >
              <span className="font-medium text-neutral-300">{phase.phase}</span>
              <span className="mx-2 text-neutral-700">·</span>
              <span>{phase.message}</span>
            </div>
          ))}
        </div>

        {liveRun.plan ? (
          <section className="mt-4 rounded-2xl border border-neutral-800 bg-neutral-950/70 p-4">
            <div className="flex items-center justify-between gap-3">
              <strong className="text-sm font-semibold text-neutral-100">
                {liveRun.plan.chart_title}
              </strong>
              <span className="rounded-full border border-neutral-700 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-neutral-400">
                {liveRun.plan.confidence}
              </span>
            </div>
            <p className="mt-3 text-sm leading-6 text-neutral-400">
              {liveRun.plan.data_description}
            </p>
          </section>
        ) : null}

        {liveRun.answer ? (
          <div className="mt-4 whitespace-pre-wrap text-sm leading-7 sm:text-[15px]">
            {liveRun.answer.answer}
          </div>
        ) : null}

        {liveRun.answer?.note ? (
          <p className="mt-3 text-xs leading-6 text-neutral-400">{liveRun.answer.note}</p>
        ) : null}

        {liveRun.chart && liveRun.preview ? (
          <div className="mt-4">
            <Suspense
              fallback={
                <div className="rounded-2xl border border-neutral-800 bg-neutral-950/70 p-4 text-sm text-neutral-500">
                  Loading chart…
                </div>
              }
            >
              <ChartPanel chart={liveRun.chart} preview={liveRun.preview} />
            </Suspense>
          </div>
        ) : null}

        {liveRun.sql ? (
          <details className="mt-4 rounded-2xl border border-neutral-800 bg-neutral-950/70 p-3">
            <summary className="cursor-pointer text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
              SQL
            </summary>
            <pre className="mt-3 overflow-x-auto whitespace-pre-wrap font-mono text-xs leading-6 text-neutral-300">
              {liveRun.sql}
            </pre>
          </details>
        ) : null}

        {liveRun.error ? (
          <p className="mt-4 text-sm text-red-300">{liveRun.error}</p>
        ) : null}
      </div>
    </article>
  );
}
