import { useEffect, useState } from "react";
import { Database, Plus, RefreshCw } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Card, Button, Spinner, Modal, TextField } from "../../components/ui";
import { StatCards, StatusPill, EmptyState } from "../../components/segment/kit";
import { useTenant } from "../../context/TenantContext";
import { useLang } from "../../context/LangContext";
import { listWarehouses, createWarehouse, syncWarehouse, type Warehouse } from "../../api/connections";
import { connectorByKey, groupBySurface, categoryLabel } from "../../lib/connectors";

function tone(s: string) {
  if (s === "healthy") return "green" as const;
  if (s === "testing") return "amber" as const;
  return "gray" as const;
}

export default function WarehousesPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const [items, setItems] = useState<Warehouse[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [name, setName] = useState("");
  const [type, setType] = useState("doris");
  const [conn, setConn] = useState("");
  const [db, setDb] = useState("");
  const [syncing, setSyncing] = useState<Record<string, boolean>>({});

  function load() {
    setItems(null); setErr(null);
    listWarehouses(tenant).then(setItems).catch((e) => setErr(String(e)));
  }
  useEffect(load, [tenant]);

  async function submit() {
    if (!name.trim()) return;
    setBusy(true); setErr(null);
    try {
      await createWarehouse(tenant, { warehouse_name: name.trim(), warehouse_type: type, connection_string: conn, database_name: db });
      setName(""); setConn(""); setDb(""); setOpen(false); load();
    } catch (e) { setErr(String(e)); } finally { setBusy(false); }
  }

  async function doSync(id: string) {
    setSyncing((p) => ({ ...p, [id]: true }));
    try { await syncWarehouse(tenant, id); load(); }
    catch (e) { setErr(String(e)); } finally { setSyncing((p) => ({ ...p, [id]: false })); }
  }

  return (
    <Layout
      title={tr("数据仓库 Warehouses", "Warehouses")}
      subtitle={tr("将 Profiles、事件与受众同步落库到 OLAP / 业务库", "Sync Profiles, events and audiences into your OLAP / operational warehouse")}
      actions={<Button onClick={() => setOpen(true)}><Plus className="h-4 w-4" /> {tr("连接数据仓库", "Connect warehouse")}</Button>}
    >
      {items && (
        <StatCards items={[
          { label: tr("数据仓库", "Warehouses"), value: items.length },
          { label: tr("健康", "Healthy"), value: items.filter((w) => w.status === "healthy").length },
          { label: tr("测试中", "Testing"), value: items.filter((w) => w.status === "testing").length },
          { label: tr("类型", "Types"), value: new Set(items.map((w) => w.warehouse_type)).size },
        ]} />
      )}

      {err && <Card className="mb-4 p-4 text-sm text-red-600">{err}</Card>}
      {!items && !err && <div className="flex items-center gap-2 text-gray-500"><Spinner /> {tr("加载中…", "Loading…")}</div>}

      {items && items.length === 0 && (
        <EmptyState icon={Database} title={tr("还没有连接数据仓库", "No warehouses connected yet")} desc={tr("连接 Doris / MySQL / Postgres / Hive，把统一数据落库同步。", "Connect Doris / MySQL / Postgres / Hive to sync your unified data into a warehouse.")}
          action={<Button onClick={() => setOpen(true)}><Plus className="h-4 w-4" /> {tr("连接数据仓库", "Connect warehouse")}</Button>} />
      )}

      {items && items.length > 0 && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {items.map((w) => (
            <Card key={w.warehouse_id} className="flex h-full flex-col p-5">
              <div className="mb-3 flex items-center justify-between">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-brand-50 text-brand-600">
                  {(() => { const Icon = connectorByKey(w.warehouse_type)?.icon ?? Database; return <Icon className="h-5 w-5" />; })()}
                </div>
                <StatusPill tone={tone(w.status)}>{w.status}</StatusPill>
              </div>
              <div className="font-semibold text-gray-900">{w.warehouse_name}</div>
              <div className="text-[11px] uppercase tracking-wide text-gray-400">{connectorByKey(w.warehouse_type)?.label ?? w.warehouse_type}</div>
              <div className="mt-1 text-sm text-gray-500">{tr("最近同步", "Last sync")} {w.last_sync_time || "—"}</div>
              {w.tables_synced && w.tables_synced.length > 0 && (
                <div className="mt-1 text-xs text-gray-400">{tr("已同步", "Synced")} {w.tables_synced.length} {tr("张表", "tables")}</div>
              )}
              <div className="mt-4">
                <Button variant="outline" onClick={() => doSync(w.warehouse_id)} disabled={syncing[w.warehouse_id]}>
                  {syncing[w.warehouse_id] ? <Spinner /> : <RefreshCw className="h-4 w-4" />} {tr("立即同步", "Sync now")}
                </Button>
              </div>
            </Card>
          ))}
        </div>
      )}

      <Modal open={open} title={tr("连接数据仓库", "Connect warehouse")} onClose={() => setOpen(false)}>
        <div className="space-y-4">
          <TextField label={tr("名称", "Name")} value={name} onChange={setName} placeholder={tr("如：生产 Doris", "e.g. Production Doris")} />
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">{tr("类型", "Type")}</span>
            <select className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
              value={type} onChange={(e) => setType(e.target.value)}>
              {groupBySurface("warehouse").map((g) => (
                <optgroup key={g.category} label={categoryLabel(g.category, tr)}>
                  {g.items.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
                </optgroup>
              ))}
            </select>
          </label>
          <TextField label={tr("连接串", "Connection string")} value={conn} onChange={setConn} placeholder="host:port" />
          <TextField label={tr("数据库", "Database")} value={db} onChange={setDb} placeholder="database name" />
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setOpen(false)}>{tr("取消", "Cancel")}</Button>
            <Button onClick={submit} disabled={busy || !name.trim()}>
              {busy ? <Spinner /> : <Plus className="h-4 w-4" />} {tr("连接", "Connect")}
            </Button>
          </div>
        </div>
      </Modal>
    </Layout>
  );
}
