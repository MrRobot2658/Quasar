import { useMemo } from "react";
import {
  ReactFlow, ReactFlowProvider, Background, Controls,
  Handle, Position, MarkerType,
  type Node, type Edge, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Boxes, GitBranch } from "lucide-react";
import { Card } from "../ui";
import { byKey } from "../../lib/objects";
import { useLang } from "../../context/LangContext";
import type { ObjectDefinitions, ObjectDefinition } from "../../api/objects";

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

/** 对象 ER 关系图：节点为对象、连线为关系（环形布局 + fitView）。 */
export default function ObjectModelGraph({ objects, relations, height = 360 }: {
  objects: ObjectDefinition[];
  relations: ObjectDefinitions["relations"];
  height?: number;
}) {
  const { tr } = useLang();
  const graph = useMemo(() => buildGraph(objects, relations), [objects, relations]);
  return (
    <>
      <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-gray-700">
        <GitBranch className="h-4 w-4 text-brand-500" /> {tr("对象 ER 关系图", "Object ER Diagram")}
        <span className="font-normal text-gray-400">
          · {objects.length} {tr("个对象", "objects")} · {graph.edges.length} {tr("条关系", "relations")}
        </span>
      </div>
      <Card className="mb-6 overflow-hidden p-0">
        <div className="w-full" style={{ height }}>
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
    </>
  );
}
