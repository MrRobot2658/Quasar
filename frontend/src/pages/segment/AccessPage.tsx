import { useCallback, useEffect, useState } from "react";
import { Trash2, UserPlus, Plus, Users } from "lucide-react";
import Layout from "../../components/layout/Layout";
import { Button, Card, DataTable, Modal, Spinner, TextField } from "../../components/ui";
import { StatCards, StatusPill, SubTabs } from "../../components/segment/kit";
import { useTenant } from "../../context/TenantContext";
import { useLang } from "../../context/LangContext";
import {
  cancelInvitation,
  createInvitation,
  createRole,
  createTeam,
  deleteRole,
  deleteUser,
  listInvitations,
  listRoles,
  listTeams,
  listUsers,
  updateUser,
  type IamUser,
  type Invitation,
  type Role,
  type Team,
} from "../../api/settings";

const TABS = [
  { label: "通用", to: "/settings" },
  { label: "权限管理", to: "/settings/access" },
  { label: "API 令牌", to: "/settings/tokens" },
  { label: "审计日志", to: "/settings/audit" },
  { label: "MCP 设置", to: "/settings/mcp" },
];

type View = "members" | "roles" | "teams" | "invitations";
const VIEWS: [View, string][] = [
  ["members", "成员"],
  ["roles", "角色"],
  ["teams", "团队"],
  ["invitations", "邀请"],
];

function statusTone(s: string) {
  return s === "active" ? "green" : s === "pending" ? "amber" : "gray";
}
function statusLabel(s: string) {
  return s === "active" ? "活跃" : s === "pending" ? "待激活" : s === "inactive" ? "停用" : s;
}

export default function AccessPage() {
  const { tenant } = useTenant();
  const [view, setView] = useState<View>("members");

  const [users, setUsers] = useState<IamUser[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [invites, setInvites] = useState<Invitation[]>([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const [inviteOpen, setInviteOpen] = useState(false);
  const [roleOpen, setRoleOpen] = useState(false);
  const [teamOpen, setTeamOpen] = useState(false);

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const [u, r, t, i] = await Promise.all([
        listUsers({ tenant_id: tenant, limit: 200 }),
        listRoles(tenant),
        listTeams(tenant),
        listInvitations(tenant),
      ]);
      setUsers(u.data);
      setRoles(r);
      setTeams(t);
      setInvites(i);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, [tenant]);

  useEffect(() => { load(); }, [load]);

  async function act(fn: () => Promise<any>, ok: string) {
    setError(null); setMsg(null);
    try {
      await fn();
      setMsg(ok);
      await load();
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message || "操作失败");
    }
  }

  const pendingInvites = invites.filter((i) => i.status === "pending").length;

  return (
    <Layout
      title="权限管理 Access Management"
      subtitle="成员、角色、团队与邀请 —— IAM 治理"
      actions={
        <>
          {view === "roles" && <Button onClick={() => setRoleOpen(true)}><Plus className="h-4 w-4" /> 新建角色</Button>}
          {view === "teams" && <Button onClick={() => setTeamOpen(true)}><Plus className="h-4 w-4" /> 新建团队</Button>}
          {(view === "members" || view === "invitations") && (
            <Button onClick={() => setInviteOpen(true)}><UserPlus className="h-4 w-4" /> 邀请成员</Button>
          )}
        </>
      }
    >
      <SubTabs tabs={TABS.map((t) => ({ ...t, active: t.label === "权限管理" }))} />

      <StatCards items={[
        { label: "成员数", value: users.length },
        { label: "角色数", value: roles.length },
        { label: "团队数", value: teams.length },
        { label: "待处理邀请", value: pendingInvites, tone: pendingInvites ? "amber" : "gray" },
      ]} />

      {error && <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{error}</div>}
      {msg && <div className="mb-4 rounded-lg bg-brand-50 px-4 py-2 text-sm text-brand-700">{msg}</div>}

      <div className="mb-4 flex gap-1">
        {VIEWS.map(([k, label]) => (
          <button key={k} onClick={() => setView(k)}
            className={`rounded-lg px-3 py-1.5 text-sm font-medium ${view === k ? "bg-brand-50 text-brand-700" : "text-gray-500 hover:bg-gray-100"}`}>
            {label}
          </button>
        ))}
      </div>

      <Card className="p-2">
        {loading ? (
          <div className="flex justify-center py-12"><Spinner /></div>
        ) : view === "members" ? (
          <DataTable
            columns={["成员", "邮箱", "角色", "团队", "状态", ""]}
            rows={users.map((u) => ({
              成员: u.name || "—",
              邮箱: u.email,
              角色: u.role || "—",
              团队: u.teams.length ? u.teams.join(", ") : "—",
              状态: <StatusPill tone={statusTone(u.status)}>{statusLabel(u.status)}</StatusPill>,
              "": (
                <div className="flex gap-3">
                  {u.status !== "active" ? (
                    <button className="text-sm font-medium text-brand-600 hover:text-brand-700"
                      onClick={() => act(() => updateUser(u.id, { status: "active" }, tenant), "已激活成员")}>
                      激活
                    </button>
                  ) : (
                    <button className="text-sm font-medium text-gray-500 hover:text-gray-700"
                      onClick={() => act(() => updateUser(u.id, { status: "inactive" }, tenant), "已停用成员")}>
                      停用
                    </button>
                  )}
                  <button className="text-sm font-medium text-red-500 hover:text-red-600"
                    onClick={() => confirm(`移除成员 ${u.email}？`) && act(() => deleteUser(u.id, tenant), "已移除成员")}>
                    移除
                  </button>
                </div>
              ),
            }))}
          />
        ) : view === "roles" ? (
          <DataTable
            columns={["角色", "成员数", "权限范围", ""]}
            rows={roles.map((r) => ({
              角色: r.name,
              成员数: r.member_count,
              权限范围: Object.keys(r.scope || {}).length ? JSON.stringify(r.scope) : "—",
              "": (
                <button className="inline-flex items-center gap-1 text-sm font-medium text-red-500 hover:text-red-600"
                  onClick={() => confirm(`删除角色 ${r.name}？`) && act(() => deleteRole(r.id, tenant), "已删除角色")}>
                  <Trash2 className="h-3.5 w-3.5" /> 删除
                </button>
              ),
            }))}
          />
        ) : view === "teams" ? (
          <DataTable
            columns={["团队", "描述", "成员数"]}
            rows={teams.map((t) => ({
              团队: t.name,
              描述: t.description || "—",
              成员数: t.member_count,
            }))}
          />
        ) : (
          <DataTable
            columns={["邮箱", "角色", "状态", "邀请人", "过期时间", ""]}
            rows={invites.map((i) => ({
              邮箱: i.email,
              角色: i.role || "—",
              状态: <StatusPill tone={i.status === "pending" ? "amber" : i.status === "accepted" ? "green" : "gray"}>{i.status}</StatusPill>,
              邀请人: i.invited_by || "—",
              过期时间: i.expires_at,
              "": i.status === "pending" ? (
                <button className="inline-flex items-center gap-1 text-sm font-medium text-red-500 hover:text-red-600"
                  onClick={() => act(() => cancelInvitation(i.id, tenant), "已撤销邀请")}>
                  <Trash2 className="h-3.5 w-3.5" /> 撤销
                </button>
              ) : <span className="text-gray-300">—</span>,
            }))}
          />
        )}
      </Card>

      <InviteModal
        open={inviteOpen} onClose={() => setInviteOpen(false)} roles={roles}
        onDone={(text) => { setInviteOpen(false); act(async () => {}, text); }}
        tenant={tenant}
      />
      <CreateRoleModal open={roleOpen} onClose={() => setRoleOpen(false)} tenant={tenant}
        onDone={() => { setRoleOpen(false); act(async () => {}, "已创建角色"); }} />
      <CreateTeamModal open={teamOpen} onClose={() => setTeamOpen(false)} tenant={tenant}
        onDone={() => { setTeamOpen(false); act(async () => {}, "已创建团队"); }} />
    </Layout>
  );
}

function InviteModal({
  open, onClose, onDone, roles, tenant,
}: {
  open: boolean; onClose: () => void; onDone: (msg: string) => void; roles: Role[]; tenant: number;
}) {
  const [email, setEmail] = useState("");
  const [roleId, setRoleId] = useState<number | "">("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [link, setLink] = useState<string | null>(null);

  async function submit() {
    if (!email.trim()) { setErr("邮箱必填"); return; }
    if (roleId === "") { setErr("请选择角色"); return; }
    setBusy(true); setErr(null);
    try {
      const r = await createInvitation({ tenant_id: tenant, email: email.trim(), role_id: Number(roleId) });
      setLink(r.invitation_url);
      onDone(`已发送邀请至 ${email.trim()}`);
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e.message || "邀请失败");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal open={open} title="邀请成员" onClose={onClose}>
      <div className="space-y-4">
        <TextField label="邮箱" value={email} onChange={setEmail} placeholder="member@example.com" />
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-gray-700">角色</span>
          <select className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
            value={roleId} onChange={(e) => setRoleId(e.target.value ? Number(e.target.value) : "")}>
            <option value="">选择角色</option>
            {roles.map((r) => <option key={r.id} value={r.id}>{r.name}</option>)}
          </select>
        </label>
        {link && <div className="rounded-lg bg-brand-50 px-3 py-2 text-xs text-brand-700 break-all">邀请链接：{link}</div>}
        {err && <div className="text-sm text-red-600">{err}</div>}
        <div className="flex items-center justify-end gap-2 pt-2">
          <Button variant="outline" onClick={onClose}>{link ? "关闭" : "取消"}</Button>
          {!link && <Button onClick={submit} disabled={busy}>{busy ? <Spinner /> : <UserPlus className="h-4 w-4" />} 发送邀请</Button>}
        </div>
      </div>
    </Modal>
  );
}

function CreateRoleModal({
  open, onClose, onDone, tenant,
}: { open: boolean; onClose: () => void; onDone: () => void; tenant: number }) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function submit() {
    if (!name.trim()) { setErr("角色名必填"); return; }
    setBusy(true); setErr(null);
    try {
      await createRole({ tenant_id: tenant, name: name.trim(), scope: {} });
      setName("");
      onDone();
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e.message || "创建失败");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal open={open} title="新建角色" onClose={onClose}>
      <div className="space-y-4">
        <TextField label="角色名称" value={name} onChange={setName} placeholder="如：分析师" />
        {err && <div className="text-sm text-red-600">{err}</div>}
        <div className="flex items-center justify-end gap-2 pt-2">
          <Button variant="outline" onClick={onClose}>取消</Button>
          <Button onClick={submit} disabled={busy}>{busy ? <Spinner /> : <Plus className="h-4 w-4" />} 创建</Button>
        </div>
      </div>
    </Modal>
  );
}

function CreateTeamModal({
  open, onClose, onDone, tenant,
}: { open: boolean; onClose: () => void; onDone: () => void; tenant: number }) {
  const [name, setName] = useState("");
  const [desc, setDesc] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function submit() {
    if (!name.trim()) { setErr("团队名必填"); return; }
    setBusy(true); setErr(null);
    try {
      await createTeam({ tenant_id: tenant, name: name.trim(), description: desc || null });
      setName(""); setDesc("");
      onDone();
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e.message || "创建失败");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal open={open} title="新建团队" onClose={onClose}>
      <div className="space-y-4">
        <TextField label="团队名称" value={name} onChange={setName} placeholder="如：增长团队" />
        <TextField label="描述" value={desc} onChange={setDesc} placeholder="可选" />
        {err && <div className="text-sm text-red-600">{err}</div>}
        <div className="flex items-center justify-end gap-2 pt-2">
          <Button variant="outline" onClick={onClose}>取消</Button>
          <Button onClick={submit} disabled={busy}>{busy ? <Spinner /> : <Users className="h-4 w-4" />} 创建</Button>
        </div>
      </div>
    </Modal>
  );
}
