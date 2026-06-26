import axios from "axios";
import type {
  DraftResult,
  DslRule,
  Metadata,
  Relation,
  SearchResult,
} from "./types";
import { tracker } from "../lib/tracker";

// 开发态 baseURL 走 vite 代理 /api → sql-engine；生产同源由 nginx 转发。
export const http = axios.create({ baseURL: "/api", timeout: 45000 });

// 登录态：每次请求自动携带 Bearer token（由 AuthContext 写入 localStorage）。
http.interceptors.request.use((config) => {
  const token = typeof localStorage !== "undefined" ? localStorage.getItem("cdp_token") : null;
  if (token) {
    config.headers = config.headers ?? {};
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// 主动式埋点：API 报错（4xx/5xx）记一条 error 行为，供 Copilot 主动提示。401 不记（登录态问题）。
http.interceptors.response.use(
  (r) => r,
  (error) => {
    try {
      const status = error?.response?.status;
      if (status && status !== 401) {
        tracker.track("error", { status, endpoint: error?.config?.url });
      }
    } catch {
      /* ignore */
    }
    return Promise.reject(error);
  },
);

export async function getMetadata(tenant: number): Promise<Metadata> {
  const { data } = await http.get(`/metadata/${tenant}/fields`);
  return data;
}

export interface SearchBody {
  tenant_id: number;
  object: string;
  conditions?: any[];
  relations?: Relation[];
  logic?: "AND" | "OR";
  limit?: number;
  count_only?: boolean;
}

export async function searchObjects(body: SearchBody): Promise<SearchResult> {
  const { data } = await http.post(`/objects/search`, { limit: 50, ...body });
  return data;
}

export async function estimate(
  tenant: number,
  rule: DslRule,
): Promise<{ estimate: number; elapsed_ms: number; sql: string }> {
  const { data } = await http.post(`/dsl/estimate`, { tenant_id: tenant, ...rule });
  return data;
}

export async function validateRule(
  tenant: number,
  rule: DslRule,
): Promise<{ ok: boolean; errors: string[] }> {
  const { data } = await http.post(`/dsl/validate`, { tenant_id: tenant, ...rule });
  return data;
}

export async function draftSegment(
  tenant: number,
  question: string,
): Promise<DraftResult> {
  const { data } = await http.post(`/agent/segment/draft`, {
    tenant_id: tenant,
    question,
  });
  return data;
}

export async function confirmSegment(
  tenant: number,
  segment_code: string,
  segment_name: string,
  rule: DslRule,
): Promise<any> {
  const { data } = await http.post(`/agent/segment/confirm`, {
    tenant_id: tenant,
    segment_code,
    segment_name,
    rule,
  });
  return data;
}

// ETL
export interface EtlFieldMap { target: string; source?: string; const?: any }
export interface EtlBody {
  tenant_id: number;
  target_object: string;
  source: { type: string; csv?: string; rows?: any[]; delimiter?: string };
  mapping: EtlFieldMap[];
  link?: { rel_type: string; dst_type: string; dst_id_source: string };
  limit_preview?: number;
}
export async function etlPreview(body: EtlBody) {
  const { data } = await http.post(`/etl/preview`, body);
  return data as {
    target_object: string; total_rows: number; source_columns: string[];
    preview: Record<string, any>[]; issues: string[];
  };
}
export async function etlImport(body: EtlBody) {
  const { data } = await http.post(`/etl/import`, body);
  return data as {
    target_object: string; total_rows: number; imported: number;
    relations: number; failed: number; errors: { row: number; error: string }[];
  };
}

// 标签 / 群组(segment)
export async function listTags(tenant: number) {
  const { data } = await http.get(`/tags/${tenant}`);
  return data as any[];
}
export async function listSegments(tenant: number) {
  const { data } = await http.get(`/segments/${tenant}`);
  return data as any[];
}

// ── 反馈闭环（docs/13-feedback-loop.md）：先触发聚合(POST)，再取展示数据(GET) ──
export async function fetchFeedback(
  tenant: number,
  topic: "segment" | "tag" | "quality" | "insight" | "field",
  objectType = "user",
): Promise<{ rows: any[]; extra?: any }> {
  const p = { params: { tenant_id: tenant } };
  if (topic === "segment") {
    await http.post(`/feedback/segment/aggregate`, null, p).catch(() => {});
    const { data } = await http.get(`/feedback/segment/quality`, p);
    return { rows: data.segments || [] };
  }
  if (topic === "tag") {
    await http.post(`/feedback/tag/aggregate`, null, p).catch(() => {});
    const { data } = await http.get(`/feedback/tag/health`, p);
    return { rows: data.tags || [] };
  }
  if (topic === "quality") {
    for (const o of ["user", "lead", "account", "order"])
      await http.post(`/feedback/quality/scan`, null, { params: { tenant_id: tenant, object_type: o } }).catch(() => {});
    const { data } = await http.get(`/feedback/quality/report`, p);
    return { rows: data.checks || [], extra: { overall: data.overall_score, byObject: data.object_scores } };
  }
  if (topic === "insight") {
    await http.post(`/feedback/insight/detect`, null, p).catch(() => {});
    const { data } = await http.get(`/feedback/insight/findings`, p);
    return { rows: data.findings || [] };
  }
  // field
  await http.post(`/feedback/field/scan`, null, { params: { tenant_id: tenant, object_type: objectType } }).catch(() => {});
  const { data } = await http.get(`/feedback/field/health`, { params: { tenant_id: tenant, object_type: objectType } });
  return { rows: data.fields || [] };
}
