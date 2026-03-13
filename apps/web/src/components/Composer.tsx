import { useState } from 'react';

interface ComposerProps {
  disabled: boolean;
  onSubmit: (question: string) => Promise<void> | void;
}

export function Composer({ disabled, onSubmit }: ComposerProps) {
  const [value, setValue] = useState('');

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const question = value.trim();
    if (!question || disabled) {
      return;
    }
    setValue('');
    await onSubmit(question);
  }

  return (
    <form
      className="mx-auto flex w-full max-w-4xl items-end gap-3 rounded-3xl border border-neutral-800 bg-neutral-900/95 p-3 shadow-[0_24px_80px_rgba(0,0,0,0.45)]"
      onSubmit={handleSubmit}
    >
      <textarea
        className="max-h-48 min-h-[72px] flex-1 resize-none bg-transparent px-3 py-2 text-sm text-neutral-100 outline-none placeholder:text-neutral-500"
        value={value}
        onChange={(event) => setValue(event.target.value)}
        placeholder="Ask about price, demand, constraints, batteries, bids, or flows..."
        rows={2}
      />
      <button
        className="rounded-2xl border border-neutral-700 bg-neutral-100 px-4 py-3 text-sm font-medium text-neutral-950 transition hover:bg-white disabled:cursor-not-allowed disabled:border-neutral-800 disabled:bg-neutral-800 disabled:text-neutral-500"
        disabled={disabled || !value.trim()}
        type="submit"
      >
        {disabled ? 'Thinking…' : 'Send'}
      </button>
    </form>
  );
}
