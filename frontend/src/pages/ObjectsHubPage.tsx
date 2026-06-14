import { useEffect, useMemo, useState } from "react";
import Layout from "../components/layout/Layout";
import { Card, Spinner } from "../components/ui";
import UnifiedFilter from "../components/filter/UnifiedFilter";
import ObjectModelGraph from "../components/objects/ObjectModelGraph";
import { byKey } from "../lib/objects";
import { useTenant } from "../context/TenantContext";
import { useLang } from "../context/LangContext";
import { getDefinitions, type ObjectDefinitions } from "../api/objects";

const objLabel = (k: string) => byKey(k)?.label ?? k;

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
          {/* 对象 ER 关系图 */}
          <ObjectModelGraph objects={defs.objects} relations={defs.relations} />

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
