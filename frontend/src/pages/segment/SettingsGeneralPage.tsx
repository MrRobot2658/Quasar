import { useCallback, useEffect, useState } from "react";
import { Save } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Button, Card, Spinner, TextField } from "../../components/ui";
import { SubTabs } from "../../components/segment/kit";
import { useTenant } from "../../context/TenantContext";
import { useLang } from "../../context/LangContext";
import { getWorkspace, updateWorkspace, type Workspace } from "../../api/settings";

export default function SettingsGeneralPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();

  const TABS = [
    { label: tr("通用", "General"), to: "/settings" },
    { label: tr("权限管理", "Access"), to: "/settings/access" },
    { label: tr("API 令牌", "API Tokens"), to: "/settings/tokens" },
    { label: tr("审计日志", "Audit Logs"), to: "/settings/audit" },
    { label: tr("MCP 设置", "MCP"), to: "/settings/mcp" },
  ];

  const [ws, setWs] = useState<Workspace | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  // 可编辑字段
  const [name, setName] = useState("");
  const [region, setRegion] = useState("");
  const [plan, setPlan] = useState("");

  const load = useCallback(async () => {
    setWs(null); setError(null); setMsg(null);
    try {
      const w = await getWorkspace(tenant);
      setWs(w);
      setName(w.name || "");
      setRegion(w.region || "");
      setPlan(w.plan || "");
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || tr("加载失败", "Failed to load"));
    }
  }, [tenant]);

  useEffect(() => { load(); }, [load]);

  async function save() {
    setSaving(true); setError(null); setMsg(null);
    try {
      const w = await updateWorkspace(tenant, { name, region, plan });
      setWs(w);
      setMsg(tr("已保存工作区设置", "Workspace settings saved"));
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || tr("保存失败", "Failed to save"));
    } finally {
      setSaving(false);
    }
  }

  const dirty = !!ws && (name !== ws.name || region !== ws.region || plan !== ws.plan);

  return (
    <Layout
      title={tr("通用", "General")}
      subtitle={tr("工作区基本信息与归属租户", "Workspace basic information and owning tenant")}
      actions={
        <Button onClick={save} disabled={!dirty || saving}>
          {saving ? <Spinner /> : <Save className="h-4 w-4" />} {tr("保存", "Save")}
        </Button>
      }
    >
      <SubTabs tabs={TABS.map((t) => ({ ...t, active: t.to === "/settings" }))} />

      {error && <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{error}</div>}
      {msg && <div className="mb-4 rounded-lg bg-brand-50 px-4 py-2 text-sm text-brand-700">{msg}</div>}

      {!ws && !error && <div className="flex items-center gap-2 text-gray-500"><Spinner /> {tr("加载中…", "Loading…")}</div>}

      {ws && (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <Card className="p-6">
            <div className="mb-4 text-base font-semibold text-gray-900">{tr("工作区", "Workspace")}</div>
            <div className="space-y-4">
              <TextField label={tr("名称", "Name")} value={name} onChange={setName} placeholder={tr("工作区名称", "Workspace name")} />
              <TextField label={tr("区域 region", "Region")} value={region} onChange={setRegion} placeholder={tr("如 cn-east", "e.g. cn-east")} />
              <TextField label={tr("套餐 plan", "Plan")} value={plan} onChange={setPlan} placeholder="standard / premium" />
            </div>
          </Card>

          <Card className="p-6">
            <div className="mb-4 text-base font-semibold text-gray-900">{tr("基础信息（只读）", "Basic Info (read-only)")}</div>
            <dl className="space-y-3">
              {[
                { k: tr("工作区 ID", "Workspace ID"), v: String(ws.id) },
                { k: tr("标识 slug", "Slug"), v: ws.slug },
                { k: tr("档位 tier", "Tier"), v: ws.tier },
                { k: "Kafka Topic", v: ws.kafka_topic || "—" },
                { k: tr("创建时间", "Created At"), v: ws.created_at },
              ].map((r) => (
                <div key={r.k} className="flex justify-between gap-4 border-b border-gray-100 py-2">
                  <dt className="text-sm text-gray-500">{r.k}</dt>
                  <dd className="text-sm font-medium text-gray-900">{r.v}</dd>
                </div>
              ))}
            </dl>
          </Card>
        </div>
      )}
    </Layout>
  );
}
