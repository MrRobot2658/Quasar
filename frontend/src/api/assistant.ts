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

export interface ChatResponse {
  reply: string;
  steps: ChatStep[];
  task: ChatTask | null;
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
): Promise<ChatResponse> {
  const { data } = await assistantHttp.post("/chat", { tenant_id, messages });
  return data;
}

export async function listAssistantTasks(): Promise<{ tasks: AssistantTask[] }> {
  const { data } = await assistantHttp.get("/tasks");
  return data;
}

export async function getMcpTools(): Promise<McpToolsResponse> {
  const { data } = await assistantHttp.get("/mcp/tools");
  return data;
}
