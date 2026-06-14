import { useEffect, useMemo, useState } from "react";
import { Boxes, Plus, Pencil, Trash2, GitBranch, Lock } from "lucide-react";
import Layout from "../components/layout/Layout";
import { Card, DataTable, Spinner, Button, Modal, TextField, Badge } from "../components/ui";
import { StatCards } from "../components/segment/kit";
import ObjectModelGraph from "../components/objects/ObjectModelGraph";
import { useTenant } from "../context/TenantContext";
import { useLang } from "../context/LangContext";
import { byKey } from "../lib/objects";
import {
  getDefinitions, createObject, addField, patchField,
  createRelation, deleteRelation,
  type ObjectDefinitions, type ObjectDefinition, type ObjectFieldDef,
} from "../api/objects";

const FIELD_TYPES = ["str", "int", "float", "datetime", "json", "json_array"];
// 类型显示名映射：接收 tr，避免在模块级调用 useLang
const typeLabels = (tr: (zh: string, en?: string) => string): Record<string, string> => ({
  str: tr("文本", "Text"), int: tr("整数", "Integer"), float: tr("小数", "Decimal"),
  datetime: tr("时间", "Datetime"), json: "JSON", json_array: tr("JSON 数组", "JSON Array"),
});
const objLabel = (k: string) => byKey(k)?.label ?? k;

// 原生下拉，样式与 TextField 对齐
function SelectField({ label, value, onChange, options }: {
  label: string; value: string; onChange: (v: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <label className="block">
      <span className="mb-1 block text-sm font-medium text-gray-700">{label}</span>
      <select
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </label>
  );
}

export default function ObjectModelPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const [defs, setDefs] = useState<ObjectDefinitions | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // 弹窗状态
  const [newObj, setNewObj] = useState(false);
  const [addFieldFor, setAddFieldFor] = useState<string | null>(null);
  const [editField, setEditField] = useState<{ obj: string; field: ObjectFieldDef } | null>(null);
  const [newRel, setNewRel] = useState(false);
  const [tab, setTab] = useState<string>("");

  const reload = () => {
    setErr(null);
    getDefinitions(tenant).then((d) => {
      setDefs(d);
      setTab((prev) => (prev && d.objects.some((o) => o.object === prev)) ? prev : (d.objects[0]?.object ?? ""));
    }).catch((e) => setErr(String(e)));
  };
  useEffect(() => { setDefs(null); reload(); /* eslint-disable-next-line */ }, [tenant]);

  const objectKeys = useMemo(() => (defs?.objects ?? []).map((o) => o.object), [defs]);
  const activeObj = useMemo(() => defs?.objects.find((o) => o.object === tab), [defs, tab]);
  const fieldTotal = useMemo(
    () => (defs?.objects ?? []).reduce((n, o) => n + (o.fields?.length ?? 0), 0),
    [defs],
  );

  // 关系表列名（列名须与行键一致才能渲染）
  const REL_COL = {
    src: tr("源对象", "Source"), rel: tr("关系", "Relation"), dst: tr("目标对象", "Target"),
    edge: tr("边字段", "Edge Fields"), ops: tr("操作", "Actions"),
  };

  return (
    <Layout
      title={tr("对象模型 Data Model", "Data Model")}
      subtitle={tr("管理对象定义、字段与跨对象关系 —— DSL 校验 / ETL / 筛选器均以此为单一事实源", "Manage object definitions, fields and cross-object relations — the single source of truth for DSL validation / ETL / filters")}
      actions={
        <>
          <Button variant="outline" onClick={() => setNewRel(true)} disabled={!defs}>
            <GitBranch className="h-4 w-4" /> {tr("新建关系", "New Relation")}
          </Button>
          <Button onClick={() => setNewObj(true)}>
            <Plus className="h-4 w-4" /> {tr("新建对象", "New Object")}
          </Button>
        </>
      }
    >
      {err && <Card className="mb-4 p-5 text-sm text-red-600">{err}</Card>}
      {!defs && !err && <div className="flex items-center gap-2 text-gray-500"><Spinner /> {tr("加载中…", "Loading…")}</div>}

      {defs && (
        <>
          {/* 对象 ER 关系图（置顶） */}
          <ObjectModelGraph objects={defs.objects} relations={defs.relations} />

          <StatCards items={[
            { label: tr("对象数", "Objects"), value: defs.objects.length },
            { label: tr("字段总数", "Total Fields"), value: fieldTotal },
            { label: tr("关系数", "Relations"), value: defs.relations.length },
            { label: tr("租户", "Tenant"), value: tenant },
          ]} />

          {/* 对象与字段：每个对象一个 Tab */}
          <div className="mb-4 flex flex-wrap gap-1 border-b border-gray-200">
            {defs.objects.map((o) => (
              <button
                key={o.object}
                onClick={() => setTab(o.object)}
                className={`-mb-px flex items-center gap-1.5 border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
                  tab === o.object
                    ? "border-brand-500 text-brand-700"
                    : "border-transparent text-gray-500 hover:text-gray-800"
                }`}
              >
                {objLabel(o.object)}
                <span className="text-[11px] text-gray-400">{o.object}</span>
              </button>
            ))}
          </div>
          {activeObj && (
            <ObjectCard
              key={activeObj.object}
              obj={activeObj}
              onAddField={() => setAddFieldFor(activeObj.object)}
              onEditField={(f) => setEditField({ obj: activeObj.object, field: f })}
            />
          )}

          {/* 关系矩阵 */}
          <div className="mb-3 mt-8 flex items-center gap-2 text-sm font-semibold text-gray-700">
            <GitBranch className="h-4 w-4 text-brand-500" /> {tr("关系矩阵 Relations", "Relations")}
            <Badge color="brand">{defs.relations.length}</Badge>
          </div>
          <Card className="p-2">
            <DataTable
              columns={[REL_COL.src, REL_COL.rel, REL_COL.dst, REL_COL.edge, REL_COL.ops]}
              rows={defs.relations.map((r) => ({
                [REL_COL.src]: objLabel(r.src_type),
                [REL_COL.rel]: r.rel_type,
                [REL_COL.dst]: objLabel(r.dst_type),
                [REL_COL.edge]: Object.keys(r.edge_fields ?? {}).join(", ") || "—",
                [REL_COL.ops]: r.builtin
                  ? <span className="inline-flex items-center gap-1 text-xs text-gray-400"><Lock className="h-3 w-3" /> {tr("内置", "Built-in")}</span>
                  : <button
                      className="inline-flex items-center gap-1 text-xs font-medium text-red-600 hover:underline"
                      onClick={async () => {
                        if (!confirm(tr(`删除关系 ${r.src_type}-${r.rel_type}->${r.dst_type}？`, `Delete relation ${r.src_type}-${r.rel_type}->${r.dst_type}?`))) return;
                        setBusy(true);
                        try { await deleteRelation(tenant, r.src_type, r.rel_type, r.dst_type); reload(); }
                        catch (e) { setErr(String(e)); } finally { setBusy(false); }
                      }}
                    ><Trash2 className="h-3 w-3" /> {tr("删除", "Delete")}</button>,
              }))}
            />
          </Card>
        </>
      )}

      {/* 新建对象 */}
      <NewObjectModal
        open={newObj}
        onClose={() => setNewObj(false)}
        busy={busy}
        onSubmit={async (body) => {
          setBusy(true);
          try { await createObject({ tenant_id: tenant, ...body }); setNewObj(false); reload(); }
          catch (e) { setErr(String(e)); } finally { setBusy(false); }
        }}
      />

      {/* 新增字段 */}
      <FieldModal
        open={!!addFieldFor}
        title={tr(`为「${objLabel(addFieldFor ?? "")}」新增字段`, `Add Field to "${objLabel(addFieldFor ?? "")}"`)}
        busy={busy}
        onClose={() => setAddFieldFor(null)}
        onSubmit={async (f) => {
          if (!addFieldFor) return;
          setBusy(true);
          try { await addField(tenant, addFieldFor, f); setAddFieldFor(null); reload(); }
          catch (e) { setErr(String(e)); } finally { setBusy(false); }
        }}
      />

      {/* 编辑字段（label/type） */}
      <FieldModal
        open={!!editField}
        title={tr(`编辑字段 ${editField?.field.code ?? ""}`, `Edit Field ${editField?.field.code ?? ""}`)}
        busy={busy}
        initial={editField?.field}
        lockCode
        onClose={() => setEditField(null)}
        onSubmit={async (f) => {
          if (!editField) return;
          setBusy(true);
          try {
            await patchField(tenant, editField.obj, editField.field.code, { type: f.type, label: f.label });
            setEditField(null); reload();
          } catch (e) { setErr(String(e)); } finally { setBusy(false); }
        }}
      />

      {/* 新建关系 */}
      <NewRelationModal
        open={newRel}
        objectKeys={objectKeys}
        busy={busy}
        onClose={() => setNewRel(false)}
        onSubmit={async (body) => {
          setBusy(true);
          try { await createRelation(tenant, body); setNewRel(false); reload(); }
          catch (e) { setErr(String(e)); } finally { setBusy(false); }
        }}
      />
    </Layout>
  );
}

// ── 子组件 ────────────────────────────────────────────────────────────
function ObjectCard({ obj, onAddField, onEditField }: {
  obj: ObjectDefinition;
  onAddField: () => void;
  onEditField: (f: ObjectFieldDef) => void;
}) {
  const { tr } = useLang();
  const TYPE_LABEL = typeLabels(tr);
  // 字段表列名（列名须与行键一致才能渲染）
  const COL = {
    code: tr("字段", "Field"), name: tr("名称", "Name"), type: tr("类型", "Type"),
    attr: tr("属性", "Attribute"), ops: tr("操作", "Actions"),
  };
  return (
    <Card className="p-5">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-brand-50 text-brand-600">
            <Boxes className="h-5 w-5" />
          </div>
          <div>
            <div className="flex items-center gap-2 font-semibold text-gray-900">
              {objLabel(obj.object)}
              <span className="text-[11px] uppercase tracking-wide text-gray-400">{obj.object}</span>
              {obj.builtin && <Badge>{tr("内置", "Built-in")}</Badge>}
            </div>
            <div className="text-xs text-gray-500">
              {tr("主键", "Primary Key")} {obj.id ?? "—"} · {obj.fields?.length ?? 0} {tr("个字段", "fields")}{obj.table ? ` · ${tr("表", "Table")} ${obj.table}` : ""}
            </div>
          </div>
        </div>
        <Button variant="outline" onClick={onAddField}>
          <Plus className="h-4 w-4" /> {tr("字段", "Field")}
        </Button>
      </div>
      <DataTable
        columns={[COL.code, COL.name, COL.type, COL.attr, COL.ops]}
        rows={(obj.fields ?? []).map((f) => ({
          [COL.code]: f.code,
          [COL.name]: f.label ?? "—",
          [COL.type]: TYPE_LABEL[f.type] ?? f.type,
          [COL.attr]: f.code === obj.id ? <Badge color="brand">{tr("主键", "Primary Key")}</Badge> : (f.builtin ? <Badge>{tr("内置", "Built-in")}</Badge> : <Badge color="green">{tr("自定义", "Custom")}</Badge>),
          [COL.ops]: (
            <button
              className="inline-flex items-center gap-1 text-xs font-medium text-brand-600 hover:underline"
              onClick={() => onEditField(f)}
            ><Pencil className="h-3 w-3" /> {tr("编辑", "Edit")}</button>
          ),
        }))}
      />
    </Card>
  );
}

function FieldModal({ open, title, onClose, onSubmit, busy, initial, lockCode }: {
  open: boolean; title: string; busy?: boolean;
  initial?: ObjectFieldDef; lockCode?: boolean;
  onClose: () => void;
  onSubmit: (f: { code: string; type: string; label?: string }) => void;
}) {
  const { tr } = useLang();
  const TYPE_LABEL = typeLabels(tr);
  const [code, setCode] = useState("");
  const [label, setLabel] = useState("");
  const [type, setType] = useState("str");
  useEffect(() => {
    if (open) { setCode(initial?.code ?? ""); setLabel(initial?.label ?? ""); setType(initial?.type ?? "str"); }
  }, [open, initial]);

  return (
    <Modal open={open} title={title} onClose={onClose}>
      <div className="space-y-4">
        {!lockCode && <TextField label={tr("字段编码 (code)", "Field Code (code)")} value={code} onChange={setCode} placeholder={tr("如 region", "e.g. region")} />}
        <TextField label={tr("中文名称", "Display Name")} value={label} onChange={setLabel} placeholder={tr("如 区域", "e.g. Region")} />
        <SelectField label={tr("类型", "Type")} value={type} onChange={setType}
          options={FIELD_TYPES.map((t) => ({ value: t, label: `${TYPE_LABEL[t]} (${t})` }))} />
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="ghost" onClick={onClose}>{tr("取消", "Cancel")}</Button>
          <Button disabled={busy || (!lockCode && !code.trim())}
            onClick={() => onSubmit({ code: code.trim(), type, label: label.trim() || undefined })}>
            {busy ? tr("提交中…", "Submitting…") : tr("保存", "Save")}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function NewObjectModal({ open, onClose, onSubmit, busy }: {
  open: boolean; busy?: boolean; onClose: () => void;
  onSubmit: (b: { object_key: string; label?: string; id_field: string; id_numeric?: boolean }) => void;
}) {
  const { tr } = useLang();
  const [key, setKey] = useState("");
  const [label, setLabel] = useState("");
  const [idField, setIdField] = useState("");
  useEffect(() => { if (open) { setKey(""); setLabel(""); setIdField(""); } }, [open]);

  return (
    <Modal open={open} title={tr("新建对象", "New Object")} onClose={onClose}>
      <div className="space-y-4">
        <TextField label={tr("对象编码 (object_key)", "Object Code (object_key)")} value={key} onChange={setKey} placeholder={tr("如 campaign", "e.g. campaign")} />
        <TextField label={tr("中文名称", "Display Name")} value={label} onChange={setLabel} placeholder={tr("如 活动", "e.g. Campaign")} />
        <TextField label={tr("主键字段 (id_field)", "Primary Key Field (id_field)")} value={idField} onChange={setIdField} placeholder={tr("如 campaign_id", "e.g. campaign_id")} />
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="ghost" onClick={onClose}>{tr("取消", "Cancel")}</Button>
          <Button disabled={busy || !key.trim() || !idField.trim()}
            onClick={() => onSubmit({ object_key: key.trim(), label: label.trim() || undefined, id_field: idField.trim() })}>
            {busy ? tr("创建中…", "Creating…") : tr("创建", "Create")}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function NewRelationModal({ open, objectKeys, onClose, onSubmit, busy }: {
  open: boolean; objectKeys: string[]; busy?: boolean;
  onClose: () => void;
  onSubmit: (b: { src_type: string; rel_type: string; dst_type: string }) => void;
}) {
  const { tr } = useLang();
  const [src, setSrc] = useState("");
  const [rel, setRel] = useState("");
  const [dst, setDst] = useState("");
  useEffect(() => {
    if (open) { setSrc(objectKeys[0] ?? ""); setRel(""); setDst(objectKeys[0] ?? ""); }
  }, [open, objectKeys]);

  const opts = objectKeys.map((k) => ({ value: k, label: `${objLabel(k)} (${k})` }));
  return (
    <Modal open={open} title={tr("新建关系", "New Relation")} onClose={onClose}>
      <div className="space-y-4">
        <SelectField label={tr("源对象", "Source")} value={src} onChange={setSrc} options={opts} />
        <TextField label={tr("关系类型 (rel_type)", "Relation Type (rel_type)")} value={rel} onChange={setRel} placeholder={tr("如 owns / visited", "e.g. owns / visited")} />
        <SelectField label={tr("目标对象", "Target")} value={dst} onChange={setDst} options={opts} />
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="ghost" onClick={onClose}>{tr("取消", "Cancel")}</Button>
          <Button disabled={busy || !src || !rel.trim() || !dst}
            onClick={() => onSubmit({ src_type: src, rel_type: rel.trim(), dst_type: dst })}>
            {busy ? tr("创建中…", "Creating…") : tr("创建", "Create")}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
