export interface AuthUser {
  id: string;
  email: string;
  name: string;
  org_id: string;
  role: string;
  permissions: string[];
  session_id: string;
  is_admin: boolean;
}

export interface MeResponse {
  user: AuthUser;
  subscription_status: string;
}

export interface ConversationSummary {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
}

export interface MessageRecord {
  id: string;
  run_id: string | null;
  role: 'user' | 'assistant';
  content: string;
  sql_text: string | null;
  metadata: AssistantMessageMetadata;
  created_at: string;
}

export interface ConversationDetail {
  conversation: ConversationSummary;
  messages: MessageRecord[];
}

export interface UsageSnapshot {
  llm_requests: number;
  clickhouse_queries: number;
  estimated_cost_usd: number;
}

export interface Plan {
  status: string;
  sql: string;
  used_objects: string[];
  data_description: string;
  note: string;
  chart_title: string;
  confidence: string;
  reason: string;
}

export interface ChartSpec {
  kind: string;
  x: string | null;
  y: string[];
  color: string | null;
  title: string;
}

export interface QueryPreview {
  columns: string[];
  rows: unknown[][];
  row_count: number;
}

export interface FinalAnswer {
  answer: string;
  note: string;
}

export interface AssistantMessageMetadata {
  sql?: string | null;
  used_objects?: string[];
  chart?: ChartSpec | null;
  preview?: QueryPreview | null;
  note?: string | null;
  confidence?: string | null;
}

export type StreamPayload =
  | { type: 'run_started'; run_id: string; conversation_id: string }
  | { type: 'status'; phase: string; message: string }
  | { type: 'plan'; plan: Plan }
  | { type: 'sql'; sql: string }
  | { type: 'query_preview'; preview: QueryPreview; chart: ChartSpec }
  | { type: 'answer'; answer: FinalAnswer }
  | { type: 'completed'; run_id: string }
  | { type: 'failed'; message: string };

export interface ChatRequest {
  question: string;
  conversation_id: string | null;
  approved_proposal: string | null;
}
