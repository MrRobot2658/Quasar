import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  FileSpreadsheet, Cloud, Plus, ArrowRight, Workflow, KeyRound, Copy,
} from "lucide-react";
import Layout from "../components/layout/Layout";
import { Card, Button, Spinner, Modal, TextField } from "../components/ui";
import { StatCards, StatusPill, EmptyState } from "../components/segment/kit";
import { useTenant } from "../context/TenantContext";
import { useLang } from "../context/LangContext";
import { listSources, createSource, type Source } from "../api/connections";
import { connectorByKey, groupBySurface, categoryLabel } from "../lib/connectors";

function statusTone(s: string) {
  if (s === "active") return "green" as const;
  if (s === "paused" || s === "disabled") return "gray" as const;
  return "amber" as const;
}

export default function ConnectionsPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const [sources, setSources] = useState<Source[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [type, setType] = useState("csv");
  const [busy, setBusy] = useState(false);
  const [created, setCreated] = useState<{ source_id: string; write_key: string } | null>(null);

  function load() {
    setSources(null); setErr(null);
    listSources(tenant).then(setSources).catch((e) => setErr(String(e)));
  }
  useEffect(load, [tenant]);

  async function submit() {
    if (!name.trim()) return;
    setBusy(true); setErr(null);
    try {
      const r = await createSource(tenant, { source_name: name.trim(), source_type: type });
      setCreated({ source_id: r.source_id, write_key: r.write_key });
      setName("");
      load();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Layout
      title={tr("数据源 Sources", "Sources")}
      subtitle={tr("把数据接入数据底座 —— 一次接入，导入任意对象（Track once, send everywhere）", "Connect data to the data foundation — track once, send everywhere")}
      actions={
        <>
          <Link to="/connections/flow">
            <Button variant="outline"><Workflow className="h-4 w-4" /> {tr("可视化编排", "Visual Flow")}</Button>
          </Link>
          <Button onClick={() => { setCreated(null); setOpen(true); }}>
            <Plus className="h-4 w-4" /> {tr("添加数据源", "Add Source")}
          </Button>
        </>
      }
    >
      {sources && (
        <StatCards items={[
          { label: tr("数据源总数", "Total Sources"), value: sources.length },
          { label: tr("活跃", "Active"), value: sources.filter((s) => s.status === "active").length },
          { label: tr("近24h事件", "Events (24h)"), value: sources.reduce((a, s) => a + (s.event_count_24h || 0), 0).toLocaleString() },
          { label: tr("类型", "Types"), value: new Set(sources.map((s) => s.source_type)).size },
        ]} />
      )}

      {err && <Card className="mb-4 p-4 text-sm text-red-600">{err}</Card>}
      {!sources && !err && <div className="flex items-center gap-2 text-gray-500"><Spinner /> {tr("加载中…", "Loading…")}</div>}

      {sources && sources.length === 0 && (
        <EmptyState
          icon={FileSpreadsheet}
          title={tr("还没有数据源", "No sources yet")}
          desc={tr("添加一个数据源开始接入数据，或用可视化 ETL 直接导入 CSV。", "Add a source to start ingesting data, or import a CSV directly with visual ETL.")}
          action={
            <div className="flex gap-2">
              <Button onClick={() => { setCreated(null); setOpen(true); }}><Plus className="h-4 w-4" /> {tr("添加数据源", "Add Source")}</Button>
              <Link to="/connections/sources/new"><Button variant="outline">{tr("CSV 导入", "Import CSV")}</Button></Link>
            </div>
          }
        />
      )}

      {sources && sources.length > 0 && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {sources.map((s) => {
            const c = connectorByKey(s.source_type);
            const meta = { icon: c?.icon ?? Cloud, label: c?.label ?? s.source_type };
            return (
              <Link key={s.source_id} to={`/connections/sources/${s.source_id}`}>
                <Card className="flex h-full flex-col p-5 transition-shadow hover:shadow-md">
                  <div className="mb-3 flex items-center justify-between">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-brand-50 text-brand-600">
                      <meta.icon className="h-5 w-5" />
                    </div>
                    <StatusPill tone={statusTone(s.status)}>{s.status}</StatusPill>
                  </div>
                  <div className="font-semibold text-gray-900">{s.source_name}</div>
                  <div className="text-[11px] uppercase tracking-wide text-gray-400">{meta.label}</div>
                  <div className="mt-2 text-sm text-gray-500">
                    {tr("近24h", "Last 24h")} <span className="font-medium text-gray-700">{(s.event_count_24h || 0).toLocaleString()}</span> {tr("事件", "events")}
                  </div>
                  <div className="mt-4 flex items-center gap-1 text-sm font-medium text-brand-600">
                    {tr("查看详情", "View details")} <ArrowRight className="h-4 w-4" />
                  </div>
                </Card>
              </Link>
            );
          })}
        </div>
      )}

      <Modal open={open} title={created ? tr("数据源已创建", "Source created") : tr("添加数据源", "Add Source")} onClose={() => setOpen(false)}>
        {created ? (
          <div className="space-y-4">
            <div className="rounded-lg bg-amber-50 p-3 text-sm text-amber-700">
              <div className="mb-1 flex items-center gap-1 font-medium"><KeyRound className="h-4 w-4" /> {tr("Write Key（仅此一次完整展示）", "Write Key (shown in full only once)")}</div>
              <div className="flex items-center gap-2">
                <code className="flex-1 break-all rounded bg-white px-2 py-1 font-mono text-xs">{created.write_key}</code>
                <button
                  className="rounded p-1 text-amber-700 hover:bg-amber-100"
                  onClick={() => navigator.clipboard?.writeText(created.write_key)}
                  title={tr("复制", "Copy")}
                ><Copy className="h-4 w-4" /></button>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <Link to={`/connections/sources/${created.source_id}`}><Button>{tr("查看详情", "View details")}</Button></Link>
              <Button variant="outline" onClick={() => setOpen(false)}>{tr("关闭", "Close")}</Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <TextField label={tr("名称", "Name")} value={name} onChange={setName} placeholder={tr("如：官网埋点 / 业务库订单", "e.g. Website tracking / Order database")} />
            <label className="block">
              <span className="mb-1 block text-sm font-medium text-gray-700">{tr("类型", "Type")}</span>
              <select
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
                value={type}
                onChange={(e) => setType(e.target.value)}
              >
                {groupBySurface("source").map((g) => (
                  <optgroup key={g.category} label={categoryLabel(g.category, tr)}>
                    {g.items.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
                  </optgroup>
                ))}
              </select>
            </label>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setOpen(false)}>{tr("取消", "Cancel")}</Button>
              <Button onClick={submit} disabled={busy || !name.trim()}>
                {busy ? <Spinner /> : <Plus className="h-4 w-4" />} {tr("创建", "Create")}
              </Button>
            </div>
          </div>
        )}
      </Modal>
    </Layout>
  );
}
