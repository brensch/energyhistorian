import type { ConversationSummary } from '../types';

interface ConversationListProps {
  conversations: ConversationSummary[];
  activeConversationId: string | null;
  onSelect: (conversationId: string) => void;
}

export function ConversationList({
  conversations,
  activeConversationId,
  onSelect,
}: ConversationListProps) {
  return (
    <section className="flex h-full min-h-0 flex-col overflow-hidden">
      <div className="mb-3 flex shrink-0 items-center justify-between">
        <div>
          <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
            Recents
          </div>
        </div>
        <div className="text-xs text-neutral-600">{conversations.length}</div>
      </div>

      <div className="flex min-h-0 flex-1 flex-col gap-2 overflow-y-auto pr-1">
        {conversations.length === 0 ? (
          <p className="rounded-xl border border-dashed border-neutral-800 px-3 py-4 text-sm text-neutral-500">
            No saved threads yet.
          </p>
        ) : (
          conversations.map((conversation) => {
            const isActive = activeConversationId === conversation.id;
            return (
              <button
                key={conversation.id}
                className={`rounded-xl px-3 py-2.5 text-left transition ${
                  isActive
                    ? 'border-l-2 border-neutral-500 bg-neutral-900 text-neutral-100'
                    : 'border-l-2 border-transparent bg-transparent text-neutral-400 hover:bg-neutral-900 hover:text-neutral-200'
                }`}
                onClick={() => onSelect(conversation.id)}
                type="button"
              >
                <div className="line-clamp-2 text-sm font-medium">{conversation.title}</div>
                <div className="mt-2 text-xs text-neutral-500">
                  {new Date(conversation.updated_at).toLocaleString()}
                </div>
              </button>
            );
          })
        )}
      </div>
    </section>
  );
}
