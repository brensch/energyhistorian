import { useEffect, useMemo, useRef, useState } from 'react';

import {
  createCheckout,
  createPortal,
  getConversation,
  getMe,
  getUsage,
  listConversations,
  streamChat,
} from './api';
import { useAppAuth } from './auth';
import { appConfig } from './config';
import { Composer } from './components/Composer';
import { ConversationList } from './components/ConversationList';
import { MessageThread, type LiveRunState } from './components/MessageThread';
import type {
  ConversationSummary,
  MeResponse,
  MessageRecord,
  ThreadContextMessage,
  UsageSnapshot,
} from './types';

const SUGGESTED_QUESTIONS = [
  'What was the average NSW1 spot price over the last 7 days?',
  'Which region had the highest average spot price yesterday?',
  'Show battery discharge leaders yesterday.',
  'What were the top binding constraints in the last 24 hours?',
  'How did VIC1 and SA1 prices compare yesterday?',
];

function conversationIdFromHash(hash: string) {
  const value = hash.replace(/^#/, '').trim();
  return value || null;
}

export default function App() {
  const auth = useAppAuth();
  const [me, setMe] = useState<MeResponse | null>(null);
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeConversationId, setActiveConversationId] = useState<string | null>(() => {
    if (typeof window === 'undefined') {
      return null;
    }
    return conversationIdFromHash(window.location.hash);
  });
  const [messages, setMessages] = useState<MessageRecord[]>([]);
  const [usage, setUsage] = useState<UsageSnapshot | null>(null);
  const [liveRun, setLiveRun] = useState<LiveRunState | null>(null);
  const [sidebarError, setSidebarError] = useState<string | null>(null);
  const scrollAnchorRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!auth.ready || !auth.isAuthenticated) {
      if (!auth.headers['X-Dev-User-Id']) {
        return;
      }
    }

    let cancelled = false;
    async function boot() {
      try {
        const [meResponse, conversationRows, usageResponse] = await Promise.all([
          getMe(auth.headers),
          listConversations(auth.headers),
          getUsage(auth.headers),
        ]);
        if (cancelled) {
          return;
        }
        setMe(meResponse);
        setConversations(conversationRows);
        setUsage(usageResponse);
        if (conversationRows[0]) {
          setActiveConversationId((current) => current ?? conversationRows[0].id);
        }
      } catch (error) {
        if (!cancelled) {
          setSidebarError(error instanceof Error ? error.message : 'Failed to load dashboard');
        }
      }
    }
    void boot();
    return () => {
      cancelled = true;
    };
  }, [auth.headers, auth.isAuthenticated, auth.ready]);

  useEffect(() => {
    if (!activeConversationId) {
      setMessages([]);
      return;
    }
    const conversationId = activeConversationId;
    let cancelled = false;
    async function loadConversation() {
      try {
        const detail = await getConversation(conversationId, auth.headers);
        if (!cancelled) {
          setMessages(detail.messages);
        }
      } catch (error) {
        if (!cancelled) {
          setSidebarError(
            error instanceof Error ? error.message : 'Failed to load conversation',
          );
        }
      }
    }
    void loadConversation();
    return () => {
      cancelled = true;
    };
  }, [activeConversationId, auth.headers]);

  useEffect(() => {
    scrollAnchorRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [messages, liveRun]);

  useEffect(() => {
    function syncFromHash() {
      const hashConversationId = conversationIdFromHash(window.location.hash);
      setActiveConversationId((current) =>
        current === hashConversationId ? current : hashConversationId,
      );
    }

    window.addEventListener('hashchange', syncFromHash);
    return () => {
      window.removeEventListener('hashchange', syncFromHash);
    };
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    const nextHash = activeConversationId ? `#${activeConversationId}` : '';
    if (window.location.hash !== nextHash) {
      const url = `${window.location.pathname}${window.location.search}${nextHash}`;
      window.history.replaceState(null, '', url);
    }
  }, [activeConversationId]);

  const conversationTitle = useMemo(() => {
    return conversations.find((conversation) => conversation.id === activeConversationId)?.title;
  }, [activeConversationId, conversations]);

  async function refreshSidebarState() {
    const [conversationRows, usageResponse] = await Promise.all([
      listConversations(auth.headers),
      getUsage(auth.headers),
    ]);
    setConversations(conversationRows);
    setUsage(usageResponse);
    return conversationRows;
  }

  async function handleSubmit(question: string) {
    if (liveRun?.busy) {
      return;
    }
    setSidebarError(null);
    const threadContext = messages.slice(-8).map(messageToThreadContext);
    const optimisticUserMessage: MessageRecord = {
      id: `optimistic-${Date.now()}`,
      run_id: null,
      role: 'user',
      content: question,
      sql_text: null,
      metadata: {},
      created_at: new Date().toISOString(),
    };
    setMessages((current) => [...current, optimisticUserMessage]);
    setLiveRun({
      phases: [],
      plan: null,
      sql: null,
      chart: null,
      preview: null,
      answer: null,
      error: null,
      busy: true,
    });

    try {
      await streamChat(
        auth.headers,
        {
          question,
          conversation_id: activeConversationId,
          approved_proposal: null,
          thread_context: threadContext,
        },
        (message) => {
          setLiveRun((current) => {
            const base: LiveRunState = current ?? {
              phases: [],
              plan: null,
              sql: null,
              chart: null,
              preview: null,
              answer: null,
              error: null,
              busy: true,
            };
            switch (message.type) {
              case 'run_started':
                if (!activeConversationId) {
                  setActiveConversationId(message.conversation_id);
                }
                return base;
              case 'status':
                return {
                  ...base,
                  phases: [...base.phases, { phase: message.phase, message: message.message }],
                };
              case 'plan':
                return { ...base, plan: message.plan };
              case 'sql':
                return { ...base, sql: message.sql };
              case 'query_preview':
                return {
                  ...base,
                  chart: message.chart,
                  preview: message.preview,
                };
              case 'answer':
                return { ...base, answer: message.answer, busy: false };
              case 'completed':
                return { ...base, busy: false };
              case 'failed':
                return { ...base, error: message.message, busy: false };
              default:
                return base;
            }
          });
        },
      );

      const conversationRows = await refreshSidebarState();
      const targetConversationId = activeConversationId ?? conversationRows[0]?.id ?? null;
      if (targetConversationId) {
        const detail = await getConversation(targetConversationId, auth.headers);
        setMessages(detail.messages);
        setActiveConversationId(targetConversationId);
      }
      setLiveRun(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Chat request failed';
      setLiveRun((current) =>
        current
          ? {
              ...current,
              error: message,
              busy: false,
            }
          : null,
      );
    }
  }

  async function handleUpgrade() {
    const { url } = await createCheckout(auth.headers);
    window.location.href = url;
  }

  async function handleManageBilling() {
    const { url } = await createPortal(auth.headers);
    window.location.href = url;
  }

  function handleNewChat() {
    setActiveConversationId(null);
    setMessages([]);
    setLiveRun(null);
  }

  if (!auth.ready) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-neutral-900 text-neutral-400">
        Loading authentication…
      </div>
    );
  }

  if (!auth.isAuthenticated && !auth.headers['X-Dev-User-Id']) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-neutral-900 p-6">
        <section className="w-full max-w-2xl rounded-[2rem] border border-neutral-800 bg-neutral-900/80 p-10 shadow-[0_30px_100px_rgba(0,0,0,0.4)]">
          <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
            NEM Explorer
          </div>
          <h1 className="mt-3 text-5xl font-semibold tracking-tight text-neutral-50">
            Warehouse-native energy market analysis.
          </h1>
          <p className="mt-4 max-w-xl text-base leading-8 text-neutral-400">
            Chat with the semantic warehouse, inspect the SQL, and manage customer usage from a
            single interface.
          </p>
          <button
            className="mt-8 rounded-2xl border border-neutral-700 bg-neutral-100 px-5 py-3 text-sm font-medium text-neutral-950 transition hover:bg-white"
            onClick={auth.signIn}
            type="button"
          >
            Sign in with WorkOS
          </button>
        </section>
      </main>
    );
  }

  return (
    <main className="flex h-screen bg-neutral-900 text-neutral-100">
      <aside className="relative z-10 hidden w-[320px] shrink-0 flex-col overflow-hidden border-r border-neutral-900 bg-neutral-950 px-4 py-5 lg:flex">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
              Energy Historian
            </div>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight text-neutral-50">
              NEM Explorer
            </h1>
          </div>
          <button
            className="rounded-xl border border-neutral-800 bg-neutral-900/70 px-3 py-2 text-xs font-medium text-neutral-300 transition hover:bg-neutral-900"
            onClick={handleNewChat}
            type="button"
          >
            New chat
          </button>
        </div>

        <p className="mt-4 text-sm leading-6 text-neutral-500">
          Semantic warehouse chat with inline charts and SQL-backed answers.
        </p>

        <div className="mt-8 flex min-h-0 flex-1 flex-col overflow-hidden">
          <div className="min-h-0 flex-1 overflow-hidden">
            <ConversationList
              activeConversationId={activeConversationId}
              conversations={conversations}
              onSelect={setActiveConversationId}
            />
          </div>

          <section className="mt-6 shrink-0 border-t border-neutral-900 pt-6">
            <div className="mb-3 text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
              Suggested
            </div>
            <div className="max-h-56 space-y-1 overflow-y-auto pr-1">
              {SUGGESTED_QUESTIONS.map((question) => (
                <button
                  key={question}
                  className="block w-full rounded-xl border border-transparent bg-transparent px-3 py-2.5 text-left text-sm leading-6 text-neutral-400 transition hover:border-neutral-800 hover:bg-neutral-900 hover:text-neutral-200"
                  onClick={() => {
                    if (!liveRun?.busy) {
                      void handleSubmit(question);
                    }
                  }}
                  type="button"
                >
                  {question}
                </button>
              ))}
            </div>
          </section>
        </div>

        <section className="mt-6 shrink-0 rounded-[1.5rem] border border-neutral-900 bg-neutral-900/80 p-4">
          <div className="flex items-center justify-between">
            <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
              Usage
            </div>
            <div className="text-xs text-neutral-400">{me?.subscription_status ?? 'inactive'}</div>
          </div>

          <div className="mt-4 space-y-3 text-sm">
            <div className="flex items-center justify-between rounded-xl border border-neutral-900 bg-neutral-950/70 px-3 py-3">
              <span className="text-neutral-500">LLM requests</span>
              <strong>{usage?.llm_requests ?? 0}</strong>
            </div>
            <div className="flex items-center justify-between rounded-xl border border-neutral-900 bg-neutral-950/70 px-3 py-3">
              <span className="text-neutral-500">Warehouse queries</span>
              <strong>{usage?.clickhouse_queries ?? 0}</strong>
            </div>
            <div className="flex items-center justify-between rounded-xl border border-neutral-900 bg-neutral-950/70 px-3 py-3">
              <span className="text-neutral-500">Estimated cost</span>
              <strong>${(usage?.estimated_cost_usd ?? 0).toFixed(2)}</strong>
            </div>
          </div>

          <div className="mt-4 border-t border-neutral-800 pt-4">
            <div className="text-sm font-medium text-neutral-200">
              {me?.user.name ?? auth.user?.name}
            </div>
            <div className="mt-1 text-xs text-neutral-500">
              {me?.user.email ?? auth.user?.email}
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              <button
                className="rounded-xl border border-neutral-800 bg-neutral-950 px-3 py-2 text-xs font-medium text-neutral-300 transition hover:bg-neutral-900"
                onClick={handleUpgrade}
                type="button"
              >
                Upgrade
              </button>
              <button
                className="rounded-xl border border-neutral-800 bg-neutral-950 px-3 py-2 text-xs font-medium text-neutral-300 transition hover:bg-neutral-900"
                onClick={handleManageBilling}
                type="button"
              >
                Billing
              </button>
              {!appConfig.enableDevAuth ? (
                <button
                  className="rounded-xl border border-neutral-800 bg-neutral-950 px-3 py-2 text-xs font-medium text-neutral-300 transition hover:bg-neutral-900"
                  onClick={auth.signOut}
                  type="button"
                >
                  Sign out
                </button>
              ) : null}
            </div>
            {sidebarError ? <p className="mt-3 text-xs text-red-300">{sidebarError}</p> : null}
          </div>
        </section>
      </aside>

      <section className="flex min-w-0 flex-1 flex-col bg-neutral-900">
        <header className="border-b border-neutral-800 bg-neutral-900 px-4 py-4 sm:px-6">
          <div className="mx-auto flex w-full max-w-4xl items-center justify-between gap-4">
            <div>
              <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
                {conversationTitle ? 'Conversation' : 'New chat'}
              </div>
              <div className="mt-1 text-sm text-neutral-300 sm:text-base">
                {conversationTitle ?? 'Start with one of the suggested prompts or ask your own.'}
              </div>
            </div>
            <button
              className="rounded-xl border border-neutral-800 bg-neutral-900 px-3 py-2 text-xs font-medium text-neutral-300 transition hover:bg-neutral-800 lg:hidden"
              onClick={handleNewChat}
              type="button"
            >
              New chat
            </button>
          </div>
        </header>

        <div className="min-h-0 flex-1 overflow-y-auto">
          <MessageThread liveRun={liveRun} messages={messages} />
          <div ref={scrollAnchorRef} />
        </div>

        <div className="border-t border-neutral-800 bg-gradient-to-t from-neutral-900 via-neutral-900 to-neutral-900/95 px-4 py-4 sm:px-6">
          <Composer disabled={Boolean(liveRun?.busy)} onSubmit={handleSubmit} />
        </div>
      </section>
    </main>
  );
}

function messageToThreadContext(message: MessageRecord): ThreadContextMessage {
  return {
    role: message.role,
    content: message.content,
    sql_text: message.sql_text,
    metadata: {
      sql: message.metadata?.sql ?? null,
      used_objects: message.metadata?.used_objects ?? [],
      chart: message.metadata?.chart ?? null,
      preview: message.metadata?.preview ?? null,
      note: message.metadata?.note ?? null,
      confidence: message.metadata?.confidence ?? null,
    },
  };
}
