import axios from "axios";

// 智能助手服务（sibling service）—— 开发态走 vite 代理 /assistant → assistant:8004，生产同源由 nginx 转发。
export const assistantHttp = axios.create({ baseURL: "/assistant", timeout: 60000 });

export type ChatRole = "user" | "assistant" | "system";
export interface ChatMessage {
  role: ChatRole;
  content: string;
}

export interface ChatStep {
  tool: string;
  args: Record<string, any>;
  ok: boolean;
  summary: string;
}

export interface ChatTask {
  run_id: string;
  job_id: string;
  status: string;
  task_name: string;
}

export interface ChatCreated {
  kind: "chart" | "dashboard";
  id: string;
  title: string;
  path: string;
}

export interface ChatNavigate {
  path: string;
  name: string;
}

// chat-native：agent 返回的内联渲染指令，前端据 type 渲染对应卡片（自行取数）。
export type ChatView =
  | { type: "profile"; one_id: number | string }
  | { type: "audience"; query: string }
  | { type: "table"; object: string; query?: string }
  | { type: "chart"; question: string }
  | { type: "chain"; object: string; id: string; max_hops?: number }
  | { type: "feedback"; topic: "segment" | "tag" | "quality" | "insight" | "field"; object_type?: string };

export interface ChatResponse {
  reply: string;
  steps: ChatStep[];
  task: ChatTask | null;
  agent?: string;
  agent_name?: string;
  created?: ChatCreated | null;
  navigate?: ChatNavigate | null;
  views?: ChatView[];
}

export interface AssistantTask {
  run_id: string;
  job_id: string;
  task_name: string;
  source_object: string;
  tenant_id: number;
  status: string;
}

export interface McpTool {
  name: string;
  description: string;
  parameters: { properties?: Record<string, any>; [k: string]: any };
}

export interface McpToolsResponse {
  server: { name: string; transport: string; path: string };
  tools: McpTool[];
  error?: string;
}

export type ChatMode = "agent" | "ask";

export async function chatAssistant(
  tenant_id: number,
  messages: ChatMessage[],
  opts?: { user_id?: number; conversation_id?: string; mode?: ChatMode },
): Promise<ChatResponse> {
  const { data } = await assistantHttp.post("/chat", {
    tenant_id, messages,
    user_id: opts?.user_id,
    conversation_id: opts?.conversation_id,
    mode: opts?.mode ?? "agent",
  });
  return data;
}

export interface HistoryMessage {
  role: "user" | "assistant";
  content: string;
  agent?: string | null;
  created_at?: string | null;
}

export async function getAssistantHistory(
  user_id: number, tenant_id: number, conversation_id?: string, limit = 50,
): Promise<HistoryMessage[]> {
  const { data } = await assistantHttp.get("/history", { params: { user_id, tenant_id, conversation_id, limit } });
  return data.messages || [];
}

export async function clearAssistantHistory(user_id: number, tenant_id: number, conversation_id?: string): Promise<void> {
  await assistantHttp.delete("/history", { params: { user_id, tenant_id, conversation_id } });
}

export interface Conversation {
  conversation_id: string;
  title: string;
  updated_at?: string | null;
  count?: number;
}

export async function listConversations(user_id: number, tenant_id: number): Promise<Conversation[]> {
  const { data } = await assistantHttp.get("/conversations", { params: { user_id, tenant_id } });
  return data.conversations || [];
}

export async function listAssistantTasks(): Promise<{ tasks: AssistantTask[] }> {
  const { data } = await assistantHttp.get("/tasks");
  return data;
}

export async function getMcpTools(): Promise<McpToolsResponse> {
  const { data } = await assistantHttp.get("/mcp/tools");
  return data;
}

export interface AgentDef { key: string; name: string; desc: string }
export async function getAgents(): Promise<AgentDef[]> {
  const { data } = await assistantHttp.get("/agents");
  return (data.agents ?? []) as AgentDef[];
}

// ── 主动式埋点 Copilot ────────────────────────────────────────────────────────
export interface BehaviorEvent {
  type: "page_view" | "click" | "search" | "empty_state" | "error" | "idle" | "repeat";
  path?: string;
  name?: string;
  ts?: number;
  payload?: Record<string, any>;
}

export interface SuggestionAction {
  type: "open_page" | "prefill" | "none";
  path?: string;
  text?: string;
}

export interface ProactiveSuggestion {
  title: string;
  message: string;
  action?: SuggestionAction;
  confidence?: number;
  signal?: string;
}

export async function observeBehavior(body: {
  tenant_id: number;
  user_id?: number;
  session_id: string;
  page?: { path: string; name?: string };
  events: BehaviorEvent[];
}): Promise<{ suggestion: ProactiveSuggestion | null }> {
  const { data } = await assistantHttp.post("/observe", body);
  return data;
}
