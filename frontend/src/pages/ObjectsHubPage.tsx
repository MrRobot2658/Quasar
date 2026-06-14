import { useEffect, useMemo, useState } from "react";
import {
  ReactFlow, ReactFlowProvider, Background, Controls,
  Handle, Position, MarkerType,
  type Node, type Edge, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Boxes, GitBranch } from "lucide-react";
import Layout from "../components/layout/Layout";
import { Card, Spinner } from "../components/ui";
import UnifiedFilter from "../components/filter/UnifiedFilter";
import { byKey } from "../lib/objects";
import { useTenant } from "../context/TenantContext";
import { useLang } from "../context/LangContext";
import { getDefinitions, type ObjectDefinitions, type ObjectDefinition } from "../api/objects";

const objLabel = (k: string) => byKey(k)?.label ?? k;

// ── 关系图节点：对象卡片（含字段数 / 内置标记）─────────────────────────────
function ObjectNode({ data }: NodeProps) {
  const d = data as unknown as { label: string; okey: string; fields: number; builtin: boolean };
  const Icon = byKey(d.okey)?.icon ?? Boxes;
  return (
    <div className="w-40 rounded-xl border border-brand-300 bg-white px-3 py-2.5 shadow-card">
      <Handle type="target" position={Position.Left} className="!h-2.5 !w-2.5 !border-2 !border-white !bg-brand-500" />
      <div className="flex items-center gap-2.5">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-brand-50 text-brand-600">
          <Icon className="h-4 w-4" />
        </div>
        <div className="min-w-0">
          <div className="truncate text-[13px] font-semibold text-gray-900">{d.label}</div>
          <div className="truncate text-[11px] text-gray-400">{d.okey} · {d.fields}</div>
        </div>
      </div>
      <Handle type="source" position={Position.Right} className="!h-2.5 !w-2.5 !border-2 !border-white !bg-brand-500" />
    </div>
  );
}

const nodeTypes = { obj: ObjectNode };
const edgeOpts = {
  animated: true,
  style: { stroke: "#52bd94", strokeWidth: 1.5 },
  markerEnd: { type: MarkerType.ArrowClosed, color: "#52bd94" },
  labelStyle: { fontSize: 11, fill: "#475467" },
  labelBgStyle: { fill: "#ecfbf4" },
};

// 环形布局，节点均匀分布，fitView 自动取景
function buildGraph(objects: ObjectDefinition[], relations: ObjectDefinitions["relations"]) {
  const n = objects.length || 1;
  const R = Math.max(180, n * 46);
  const cx = R + 120, cy = R + 20;
  const nodes: Node[] = objects.map((o, i) => {
    const a = (2 * Math.PI * i) / n - Math.PI / 2;
    return {
      id: o.object, type: "obj",
      position: { x: cx + R * Math.cos(a), y: cy + R * Math.sin(a) },
      data: { label: objLabel(o.object), okey: o.object, fields: (o.fields || []).length, builtin: !!o.builtin },
    };
  });
  const ids = new Set(objects.map((o) => o.object));
  const edges: Edge[] = relations
    .filter((r) => ids.has(r.src_type) && ids.has(r.dst_type))
    .map((r, i) => ({
      id: `e${i}`, source: r.src_type, target: r.dst_type, label: r.rel_type, ...edgeOpts,
    }));
  return { nodes, edges };
}

export default function ObjectsHubPage() {
  const { tenant } = useTenant();
  const { tr } = useLang();
  const [defs, setDefs] = useState<ObjectDefinitions | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tab, setTab] = useState<string>("");

  useEffect(() => {
    setDefs(null); setErr(null);
    getDefinitions(tenant).then((d) => {
      setDefs(d);
      setTab((prev) => prev || d.objects[0]?.object || "");
    }).catch((e) => setErr(String(e)));
  }, [tenant]);

  const graph = useMemo(
    () => (defs ? buildGraph(defs.objects, defs.relations) : { nodes: [], edges: [] }),
    [defs],
  );
  // 当前 tab 对象的主键字段（行点击进记录详情用）
  const idField = useMemo(
    () => defs?.objects.find((o) => o.object === tab)?.id,
    [defs, tab],
  );

  return (
    <Layout
      title={tr("对象", "Objects")}
      subtitle={tr(
        "对象模型关系图 + 各对象主数据 —— 节点为对象、连线为关系；下方按对象切换记录列表",
        "Object model graph + master data — nodes are objects, edges are relations; switch records per object below",
      )}
    >
      {err && <Card className="mb-4 p-5 text-sm text-red-600">{err}</Card>}
      {!defs && !err && (
        <div className="flex items-center gap-2 text-gray-500"><Spinner /> {tr("加载中…", "Loading…")}</div>
      )}

      {defs && (
        <>
          {/* 模型关系图 */}
          <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-gray-700">
            <GitBranch className="h-4 w-4 text-brand-500" /> {tr("模型关系图", "Model & Relations")}
            <span className="font-normal text-gray-400">
              · {defs.objects.length} {tr("个对象", "objects")} · {graph.edges.length} {tr("条关系", "relations")}
            </span>
          </div>
          <Card className="mb-6 overflow-hidden p-0">
            <div className="h-[360px] w-full">
              <ReactFlowProvider>
                <ReactFlow
                  nodes={graph.nodes}
                  edges={graph.edges}
                  nodeTypes={nodeTypes}
                  fitView
                  proOptions={{ hideAttribution: true }}
                  nodesConnectable={false}
                  edgesFocusable={false}
                >
                  <Background gap={16} color="#e4e7ec" />
                  <Controls showInteractive={false} />
                </ReactFlow>
              </ReactFlowProvider>
            </div>
          </Card>

          {/* 每个对象一个 Tab */}
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

          {/* 当前对象记录列表（复用统一筛选器，整行可点进记录详情） */}
          {tab && (
            <UnifiedFilter
              key={tab}
              baseObject={tab}
              lockBase
              autoSearch
              rowLink={(row) => (idField && row[idField] != null
                ? `/objects/${tab}/${row[idField]}`
                : undefined)}
            />
          )}
        </>
      )}
    </Layout>
  );
}
