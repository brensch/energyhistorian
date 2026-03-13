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
    <section className="min-h-0 flex-1">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <div className="text-xs font-medium uppercase tracking-[0.18em] text-neutral-500">
            Recents
          </div>
        </div>
        <div className="text-xs text-neutral-600">{conversations.length}</div>
      </div>

      <div className="flex max-h-full flex-col gap-2 overflow-y-auto pr-1">
        {conversations.length === 0 ? (
          <p className="rounded-2xl border border-dashed border-neutral-800 px-3 py-4 text-sm text-neutral-500">
            No saved threads yet.
          </p>
        ) : (
          conversations.map((conversation) => {
            const isActive = activeConversationId === conversation.id;
            return (
              <button
                key={conversation.id}
                className={`rounded-2xl border px-3 py-3 text-left transition ${
                  isActive
                    ? 'border-neutral-600 bg-neutral-800 text-neutral-100'
                    : 'border-neutral-900 bg-neutral-950 text-neutral-300 hover:border-neutral-800 hover:bg-neutral-900'
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
