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

export interface ChatResponse {
  reply: string;
  steps: ChatStep[];
  task: ChatTask | null;
  agent?: string;
  agent_name?: string;
  created?: ChatCreated | null;
  navigate?: ChatNavigate | null;
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

export async function chatAssistant(
  tenant_id: number,
  messages: ChatMessage[],
  user_id?: number,
): Promise<ChatResponse> {
  const { data } = await assistantHttp.post("/chat", { tenant_id, messages, user_id });
  return data;
}

export interface HistoryMessage {
  role: "user" | "assistant";
  content: string;
  agent?: string | null;
  created_at?: string | null;
}

export async function getAssistantHistory(user_id: number, tenant_id: number, limit = 50): Promise<HistoryMessage[]> {
  const { data } = await assistantHttp.get("/history", { params: { user_id, tenant_id, limit } });
  return data.messages || [];
}

export async function clearAssistantHistory(user_id: number, tenant_id: number): Promise<void> {
  await assistantHttp.delete("/history", { params: { user_id, tenant_id } });
}

export async function listAssistantTasks(): Promise<{ tasks: AssistantTask[] }> {
  const { data } = await assistantHttp.get("/tasks");
  return data;
}

export async function getMcpTools(): Promise<McpToolsResponse> {
  const { data } = await assistantHttp.get("/mcp/tools");
  return data;
}
