import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Building2, Plus, Search } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Button, Card, DataTable, Modal, Spinner, TextField } from "../../components/ui";
import { StatCards, StatusPill, SubTabs } from "../../components/segment/kit";
import {
  createTenant,
  listTenants,
  type TenantRow,
} from "../../api/platform";
import { useLang } from "../../context/LangContext";

// 设置区子页签（与既有 settings 页一致，新增「租户管理」）
function buildTabs(tr: (zh: string, en: string) => string) {
  return [
    { label: tr("通用", "General"), to: "/settings" },
    { label: tr("权限管理", "Access"), to: "/settings/access" },
    { label: tr("API 令牌", "API Tokens"), to: "/settings/tokens" },
    { label: tr("审计日志", "Audit Log"), to: "/settings/audit" },
    { label: tr("租户管理", "Tenants"), to: "/settings/tenants" },
    { label: tr("MCP 设置", "MCP"), to: "/settings/mcp" },
  ];
}

const TIERS = ["", "premium", "standard"];
const STATUSES = ["", "active", "suspended"];
const SCALES = ["dev", "medium", "large", "xlarge"];

function statusTone(s: string) {
  return s === "active" ? "green" : s === "suspended" ? "red" : "gray";
}

export default function TenantsPage() {
  const { tr } = useLang();
  const navigate = useNavigate();
  const [rows, setRows] = useState<TenantRow[] | null>(null);
  const [total, setTotal] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  const [search, setSearch] = useState("");
  const [tier, setTier] = useState("");
  const [status, setStatus] = useState("");

  const [createOpen, setCreateOpen] = useState(false);

  function reload() {
    setRows(null);
    setErr(null);
    listTenants({
      search: search || undefined,
      tier: tier || undefined,
      status: status || undefined,
      limit: 200,
    })
      .then((r) => {
        setRows(r.tenants);
        setTotal(r.total);
      })
      .catch((e) => setErr(String(e?.response?.data?.detail || e)));
  }

  // 筛选条件变化即重查（搜索框走回车/按钮）
  useEffect(reload, [tier, status]);
  useEffect(reload, []);

  const COL = {
    id: tr("租户ID", "Tenant ID"),
    name: tr("名称", "Name"),
    tier: tr("档位", "Tier"),
    status: tr("状态", "Status"),
    scale: tr("规模", "Scale"),
    events: tr("近24h事件", "Events (24h)"),
    contact: tr("联系人", "Contact"),
  };

  const view = (rows || []).map((r) => ({
    [COL.id]: r.tenant_id,
    [COL.name]: r.tenant_name,
    [COL.tier]: r.tier,
    [COL.status]: <StatusPill tone={statusTone(r.status)}>{r.status}</StatusPill>,
    [COL.scale]: r.scale_tier,
    [COL.events]: r.events_count_24h,
    [COL.contact]: r.contact_email || "—",
    _id: r.tenant_id,
  }));

  const activeN = (rows || []).filter((r) => r.status === "active").length;

  return (
    <Layout
      title={tr("租户管理 Tenant Management", "Tenant Management")}
      subtitle={tr("平台级多租户治理 —— 租户清单、生命周期与每租户独立配置", "Platform-level multi-tenant governance — tenant directory, lifecycle and per-tenant configuration")}
      actions={
        <Button onClick={() => setCreateOpen(true)}>
          <Plus className="h-4 w-4" /> {tr("新建租户", "New Tenant")}
        </Button>
      }
    >
      <SubTabs tabs={buildTabs(tr).map((t) => ({ ...t, active: t.to === "/settings/tenants" }))} />

      <StatCards
        items={[
          { label: tr("租户总数", "Total Tenants"), value: total },
          { label: tr("活跃", "Active"), value: activeN, tone: "green" },
          { label: tr("停用", "Suspended"), value: (rows?.length ?? 0) - activeN, tone: "red" },
        ]}
      />

      {/* 搜索 / 筛选 */}
      <Card className="mb-4 p-4">
        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-[220px] flex-1">
            <label className="block">
              <span className="mb-1 block text-sm font-medium text-gray-700">{tr("搜索", "Search")}</span>
              <div className="flex gap-2">
                <input
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
                  value={search}
                  placeholder={tr("名称或租户 ID", "Name or tenant ID")}
                  onChange={(e) => setSearch(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && reload()}
                />
                <Button variant="outline" onClick={reload}>
                  <Search className="h-4 w-4" />
                </Button>
              </div>
            </label>
          </div>
          <Select label={tr("档位 tier", "Tier")} value={tier} onChange={setTier} options={TIERS} />
          <Select label={tr("状态 status", "Status")} value={status} onChange={setStatus} options={STATUSES} />
        </div>
      </Card>

      {err && <Card className="p-5 text-sm text-red-600">{err}</Card>}
      {!rows && !err && (
        <div className="flex items-center gap-2 text-gray-500">
          <Spinner /> {tr("加载中", "Loading")}…
        </div>
      )}
      {rows && (
        <Card className="p-2">
          <div className="px-3 pb-2 pt-3 text-sm font-semibold text-gray-700">
            {tr("租户列表", "Tenant List")} <span className="ml-2 font-normal text-gray-400">· {tr("点击行进入配置中心", "Click a row to open the config center")}</span>
          </div>
          <DataTable
            columns={[COL.id, COL.name, COL.tier, COL.status, COL.scale, COL.events, COL.contact]}
            rows={view}
            rowLink={(r) => `/settings/tenants/${r._id}`}
          />
        </Card>
      )}

      <CreateTenantModal
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreated={(id) => {
          setCreateOpen(false);
          navigate(`/settings/tenants/${id}`);
        }}
      />
    </Layout>
  );
}

function Select({
  label, value, onChange, options,
}: { label: string; value: string; onChange: (v: string) => void; options: string[] }) {
  const { tr } = useLang();
  return (
    <label className="block">
      <span className="mb-1 block text-sm font-medium text-gray-700">{label}</span>
      <select
        className="rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.map((o) => (
          <option key={o} value={o}>{o === "" ? tr("全部", "All") : o}</option>
        ))}
      </select>
    </label>
  );
}

function CreateTenantModal({
  open, onClose, onCreated,
}: { open: boolean; onClose: () => void; onCreated: (id: number) => void }) {
  const { tr } = useLang();
  const [name, setName] = useState("");
  const [tier, setTier] = useState("standard");
  const [scale, setScale] = useState("dev");
  const [email, setEmail] = useState("");
  const [desc, setDesc] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function submit() {
    if (!name.trim()) {
      setErr(tr("租户名称必填", "Tenant name is required"));
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      const r = await createTenant({
        tenant_name: name.trim(),
        tier,
        scale_tier: scale,
        contact_email: email || null,
        description: desc || null,
      });
      onCreated(r.tenant_id);
    } catch (e: any) {
      setErr(String(e?.response?.data?.detail || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal open={open} title={tr("新建租户", "New Tenant")} onClose={onClose}>
      <div className="space-y-4">
        <TextField label={tr("租户名称", "Tenant Name")} value={name} onChange={setName} placeholder={tr("如：示例零售集团", "e.g. Example Retail Group")} />
        <div className="grid grid-cols-2 gap-3">
          <Select label={tr("档位 tier", "Tier")} value={tier} onChange={setTier} options={["standard", "premium"]} />
          <Select label={tr("规模 scale", "Scale")} value={scale} onChange={setScale} options={SCALES} />
        </div>
        <TextField label={tr("联系人邮箱", "Contact Email")} value={email} onChange={setEmail} placeholder={tr("可选", "Optional")} />
        <TextField label={tr("描述", "Description")} value={desc} onChange={setDesc} placeholder={tr("可选", "Optional")} />
        {err && <div className="text-sm text-red-600">{err}</div>}
        <div className="flex items-center justify-end gap-2 pt-2">
          <Button variant="outline" onClick={onClose}>{tr("取消", "Cancel")}</Button>
          <Button onClick={submit} disabled={busy}>
            {busy ? <Spinner /> : <Building2 className="h-4 w-4" />} {tr("创建", "Create")}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
