import { useCallback, useEffect, useState } from "react";
import { Search } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Button, Card, DataTable, Spinner } from "../../components/ui";
import { SubTabs } from "../../components/segment/kit";
import { useTenant } from "../../context/TenantContext";
import { useLang } from "../../context/LangContext";
import { listAudit, type AuditEntry } from "../../api/settings";

export default function AuditPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const TABS = [
    { label: tr("通用", "General"), to: "/settings" },
    { label: tr("权限管理", "Access Management"), to: "/settings/access" },
    { label: tr("API 令牌", "API Tokens"), to: "/settings/tokens" },
    { label: tr("审计日志", "Audit Log"), to: "/settings/audit" },
    { label: tr("MCP 设置", "MCP"), to: "/settings/mcp" },
  ];
  const COL = {
    time: tr("时间", "Time"),
    actor: tr("操作者", "Actor"),
    action: tr("动作", "Action"),
    target: tr("对象", "Target"),
    module: tr("模块", "Module"),
    details: tr("详情", "Details"),
  };
  const [rows, setRows] = useState<AuditEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [actor, setActor] = useState("");
  const [action, setAction] = useState("");
  const [target, setTarget] = useState("");

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const r = await listAudit({
        tenant_id: tenant,
        actor: actor || undefined,
        action: action || undefined,
        target: target || undefined,
        limit: 200,
      });
      setRows(r.data);
      setTotal(r.total);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || tr("加载失败", "Failed to load"));
    } finally {
      setLoading(false);
    }
  }, [tenant, actor, action, target]);

  // 租户切换即重查；筛选走按钮/回车
  useEffect(() => { load(); }, [tenant]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <Layout
      title={tr("审计日志 Audit Trail", "Audit Trail")}
      subtitle={tr(`工作区内的关键操作记录 · 共 ${total} 条`, `Key operation records in this workspace · ${total} total`)}
    >
      <SubTabs tabs={TABS.map((t) => ({ ...t, active: t.label === tr("审计日志", "Audit Log") }))} />

      <Card className="mb-4 p-4">
        <div className="flex flex-wrap items-end gap-3">
          <Field label={tr("操作者 actor", "Actor")} value={actor} onChange={setActor} onEnter={load} placeholder={tr("如 system", "e.g. system")} />
          <Field label={tr("动作 action", "Action")} value={action} onChange={setAction} onEnter={load} placeholder={tr("如 issue_token", "e.g. issue_token")} />
          <Field label={tr("对象 target", "Target")} value={target} onChange={setTarget} onEnter={load} placeholder={tr("模糊匹配", "Fuzzy match")} />
          <Button variant="outline" onClick={load}><Search className="h-4 w-4" /> {tr("查询", "Search")}</Button>
        </div>
      </Card>

      {error && <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{error}</div>}

      <Card className="p-2">
        {loading ? (
          <div className="flex justify-center py-12"><Spinner /></div>
        ) : (
          <DataTable
            columns={[COL.time, COL.actor, COL.action, COL.target, COL.module, COL.details]}
            rows={rows.map((a) => ({
              [COL.time]: a.time,
              [COL.actor]: a.actor,
              [COL.action]: a.action,
              [COL.target]: a.target,
              [COL.module]: a.module,
              [COL.details]: a.details && Object.keys(a.details).length
                ? <span className="text-xs text-gray-500">{JSON.stringify(a.details)}</span>
                : "—",
            }))}
          />
        )}
        {!loading && rows.length === 0 && (
          <div className="px-6 py-12 text-center text-sm text-gray-500">{tr("无审计记录", "No audit records")}</div>
        )}
      </Card>
    </Layout>
  );
}

function Field({
  label, value, onChange, onEnter, placeholder,
}: { label: string; value: string; onChange: (v: string) => void; onEnter: () => void; placeholder?: string }) {
  return (
    <label className="block">
      <span className="mb-1 block text-sm font-medium text-gray-700">{label}</span>
      <input
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && onEnter()}
      />
    </label>
  );
}
