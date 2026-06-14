import { useCallback, useEffect, useState } from "react";
import { KeyRound, Plus, Trash2 } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Button, Card, DataTable, Modal, Spinner, TextField } from "../../components/ui";
import { StatusPill, SubTabs } from "../../components/segment/kit";
import { useTenant } from "../../context/TenantContext";
import { useLang } from "../../context/LangContext";
import { issueToken, listTokens, revokeToken, type ApiToken } from "../../api/settings";

export default function TokensPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const [tokens, setTokens] = useState<ApiToken[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);

  const TABS = [
    { label: tr("通用", "General"), to: "/settings" },
    { label: tr("权限管理", "Access"), to: "/settings/access" },
    { label: tr("API 令牌", "API Tokens"), to: "/settings/tokens" },
    { label: tr("审计日志", "Audit Log"), to: "/settings/audit" },
    { label: tr("MCP 设置", "MCP"), to: "/settings/mcp" },
  ];

  const COL = {
    name: tr("名称", "Name"),
    token: tr("令牌", "Token"),
    scopes: tr("权限", "Scopes"),
    status: tr("状态", "Status"),
    created: tr("创建时间", "Created"),
    lastUsed: tr("最近使用", "Last Used"),
  };

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const r = await listTokens({ tenant_id: tenant, limit: 200 });
      setTokens(r.data);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || tr("加载失败", "Failed to load"));
    } finally {
      setLoading(false);
    }
  }, [tenant]);

  useEffect(() => { load(); }, [load]);

  async function revoke(t: ApiToken) {
    if (!confirm(tr(`吊销令牌「${t.label}」？吊销后无法恢复。`, `Revoke token "${t.label}"? This cannot be undone.`))) return;
    setError(null); setMsg(null);
    try {
      await revokeToken(t.id, tenant);
      setMsg(tr(`已吊销令牌「${t.label}」`, `Token "${t.label}" revoked`));
      await load();
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || tr("吊销失败", "Failed to revoke"));
    }
  }

  const active = tokens.filter((t) => !t.revoked_at).length;

  return (
    <Layout
      title={tr("API 令牌 API Tokens", "API Tokens")}
      subtitle={tr("服务端访问凭证与权限范围", "Server-side access credentials and permission scopes")}
      actions={<Button onClick={() => setCreateOpen(true)}><Plus className="h-4 w-4" /> {tr("生成令牌", "Generate Token")}</Button>}
    >
      <SubTabs tabs={TABS.map((t) => ({ ...t, active: t.label === tr("API 令牌", "API Tokens") }))} />

      {error && <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{error}</div>}
      {msg && <div className="mb-4 rounded-lg bg-brand-50 px-4 py-2 text-sm text-brand-700">{msg}</div>}

      <Card className="p-2">
        {loading ? (
          <div className="flex justify-center py-12"><Spinner /></div>
        ) : (
          <DataTable
            columns={[COL.name, COL.token, COL.scopes, COL.status, COL.created, COL.lastUsed, ""]}
            rows={tokens.map((t) => ({
              [COL.name]: t.label,
              [COL.token]: <code className="rounded bg-gray-100 px-1.5 py-0.5 text-xs">{t.prefix}…</code>,
              [COL.scopes]: t.scopes.length ? t.scopes.join(", ") : "—",
              [COL.status]: t.revoked_at
                ? <StatusPill tone="red">{tr("已吊销", "Revoked")}</StatusPill>
                : <StatusPill tone="green">{tr("活跃", "Active")}</StatusPill>,
              [COL.created]: t.created_at,
              [COL.lastUsed]: t.last_used || "—",
              "": t.revoked_at ? <span className="text-gray-300">—</span> : (
                <button className="inline-flex items-center gap-1 text-sm font-medium text-red-500 hover:text-red-600"
                  onClick={() => revoke(t)}>
                  <Trash2 className="h-3.5 w-3.5" /> {tr("吊销", "Revoke")}
                </button>
              ),
            }))}
          />
        )}
        {!loading && tokens.length === 0 && (
          <div className="px-6 py-12 text-center text-sm text-gray-500">{tr("暂无令牌，点击右上角「生成令牌」创建。", "No tokens yet. Click \"Generate Token\" in the top right to create one.")}</div>
        )}
      </Card>

      <CreateTokenModal open={createOpen} onClose={() => setCreateOpen(false)} tenant={tenant}
        onDone={(text) => { setMsg(text); load(); }} />
    </Layout>
  );
}

const SCOPE_OPTIONS = ["read", "write", "admin", "segments", "objects", "etl"];

function CreateTokenModal({
  open, onClose, onDone, tenant,
}: { open: boolean; onClose: () => void; onDone: (msg: string) => void; tenant: number }) {
  const { tr } = useLang();
  const [label, setLabel] = useState("");
  const [scopes, setScopes] = useState<string[]>(["read"]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [plaintext, setPlaintext] = useState<string | null>(null);

  function toggle(s: string) {
    setScopes((prev) => prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s]);
  }

  async function submit() {
    if (!label.trim()) { setErr(tr("名称必填", "Name is required")); return; }
    setBusy(true); setErr(null);
    try {
      const r = await issueToken({ tenant_id: tenant, label: label.trim(), scopes });
      setPlaintext(r.token_plaintext);
      onDone(tr(`已生成令牌「${r.label}」`, `Token "${r.label}" generated`));
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e.message || tr("生成失败", "Failed to generate"));
    } finally {
      setBusy(false);
    }
  }

  function close() {
    setLabel(""); setScopes(["read"]); setPlaintext(null); setErr(null);
    onClose();
  }

  return (
    <Modal open={open} title={tr("生成 API 令牌", "Generate API Token")} onClose={close}>
      {plaintext ? (
        <div className="space-y-4">
          <div className="rounded-lg bg-amber-50 px-3 py-2 text-sm text-amber-700 ring-1 ring-inset ring-amber-200">
            {tr("请立即复制并妥善保存，令牌明文仅显示这一次。", "Copy and store it securely now — the plaintext token is shown only once.")}
          </div>
          <code className="block break-all rounded-lg bg-gray-900 px-3 py-3 text-xs text-green-300">{plaintext}</code>
          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="outline" onClick={() => navigator.clipboard?.writeText(plaintext)}>{tr("复制", "Copy")}</Button>
            <Button onClick={close}>{tr("完成", "Done")}</Button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <TextField label={tr("名称", "Name")} value={label} onChange={setLabel} placeholder={tr("如：数据同步服务", "e.g. Data Sync Service")} />
          <div>
            <span className="mb-1 block text-sm font-medium text-gray-700">{tr("权限范围", "Scopes")}</span>
            <div className="flex flex-wrap gap-2">
              {SCOPE_OPTIONS.map((s) => (
                <button key={s} type="button" onClick={() => toggle(s)}
                  className={`rounded-full px-3 py-1 text-xs font-medium ring-1 ring-inset ${
                    scopes.includes(s) ? "bg-brand-50 text-brand-700 ring-brand-200" : "bg-gray-50 text-gray-500 ring-gray-200"}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>
          {err && <div className="text-sm text-red-600">{err}</div>}
          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="outline" onClick={close}>{tr("取消", "Cancel")}</Button>
            <Button onClick={submit} disabled={busy}>{busy ? <Spinner /> : <KeyRound className="h-4 w-4" />} {tr("生成", "Generate")}</Button>
          </div>
        </div>
      )}
    </Modal>
  );
}
