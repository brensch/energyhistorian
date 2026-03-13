import { appConfig } from './config';
import type {
  ChatRequest,
  ConversationDetail,
  ConversationSummary,
  MeResponse,
  StreamPayload,
  UsageSnapshot,
} from './types';

export async function getMe(headers: Record<string, string>) {
  return requestJson<MeResponse>('/v1/me', headers);
}

export async function listConversations(headers: Record<string, string>) {
  return requestJson<ConversationSummary[]>('/v1/conversations', headers);
}

export async function getConversation(id: string, headers: Record<string, string>) {
  return requestJson<ConversationDetail>(`/v1/conversations/${id}`, headers);
}

export async function getUsage(headers: Record<string, string>) {
  return requestJson<UsageSnapshot>('/v1/usage', headers);
}

export async function createCheckout(headers: Record<string, string>) {
  return requestJson<{ url: string }>('/v1/billing/checkout', headers, {
    method: 'POST',
  });
}

export async function createPortal(headers: Record<string, string>) {
  return requestJson<{ url: string }>('/v1/billing/portal', headers, {
    method: 'POST',
  });
}

export async function streamChat(
  headers: Record<string, string>,
  payload: ChatRequest,
  onMessage: (message: StreamPayload) => void,
) {
  const response = await fetch(`${appConfig.apiBaseUrl}/v1/chat/stream`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok || !response.body) {
    const body = await response.text().catch(() => '');
    throw new Error(body || `HTTP ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    let boundary = buffer.indexOf('\n\n');
    while (boundary >= 0) {
      const chunk = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const payload = parseSseChunk(chunk);
      if (payload) {
        onMessage(payload);
      }
      boundary = buffer.indexOf('\n\n');
    }
  }
}

async function requestJson<T>(
  path: string,
  headers: Record<string, string>,
  init?: RequestInit,
) {
  const response = await fetch(`${appConfig.apiBaseUrl}${path}`, {
    ...init,
    headers: {
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...headers,
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(body || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

function parseSseChunk(chunk: string) {
  const lines = chunk.split('\n');
  const dataLines = lines
    .filter((line) => line.startsWith('data:'))
    .map((line) => line.slice(5).trim());
  if (dataLines.length === 0) {
    return null;
  }
  return JSON.parse(dataLines.join('\n')) as StreamPayload;
}
