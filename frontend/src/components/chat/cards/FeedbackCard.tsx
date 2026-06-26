import { useEffect, useState } from "react";
import { Activity } from "lucide-react";
import { fetchFeedback } from "../../../api/client";
import { useTenant } from "../../../context/TenantContext";
import CardShell from "./CardShell";

type Topic = "segment" | "tag" | "quality" | "insight" | "field";

const TITLES: Record<Topic, string> = {
  segment: "人群触达效果",
  tag: "标签健康度",
  quality: "数据质量分",
  insight: "异常洞察",
  field: "画像字段健康",
};
const SUBTITLES: Record<Topic, string> = {
  segment: "触达回执回传 → 转化率 / 质量分",
  tag: "圈选频次 + 转化率回传 → 加权 / 归档",
  quality: "空值 / 重复 / 格式巡检 → 质量分",
  insight: "SKU 断崖 / 客群漂移 → 归因",
  field: "填充率 + 消费频次 → 补缺 / 淘汰",
};

const REC_BADGE: Record<string, string> = {
  enrich: "bg-amber-50 text-amber-700",
  deprecate: "bg-gray-100 text-gray-500",
  keep: "bg-green-50 text-green-600",
};
const REC_LABEL: Record<string, string> = { enrich: "建议补缺", deprecate: "建议淘汰", keep: "保留" };
const SEV_BADGE: Record<string, string> = {
  high: "bg-red-50 text-red-600",
  medium: "bg-amber-50 text-amber-700",
  low: "bg-green-50 text-green-600",
};

function pct(v: any) {
  return v == null ? "—" : `${Number(v).toFixed(1)}%`;
}

// 反馈闭环内联卡片：按 topic 触发聚合并渲染对应回传榜单（详见 docs/13-feedback-loop.md）。
export default function FeedbackCard({ topic, object_type = "user" }: { topic: Topic; object_type?: string }) {
  const { tenant } = useTenant();
  const [rows, setRows] = useState<any[] | null>(null);
  const [extra, setExtra] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setRows(null); setErr(null); setExtra(null);
    fetchFeedback(tenant, topic, object_type)
      .then((d) => { setRows(d.rows); setExtra(d.extra); })
      .catch((e) => setErr(e?.response?.data?.detail || String(e)));
  }, [topic, object_type, tenant]);

  const loading = rows === null && !err;
  const empty = rows !== null && rows.length === 0;

  return (
    <CardShell
      icon={<Activity className="h-4 w-4" />}
      title={TITLES[topic]}
      subtitle={SUBTITLES[topic]}
      loading={loading}
      error={err}
    >
      {empty && <div className="py-1 text-[12px] text-gray-400">暂无回传数据，先发生一次触达/巡检后再看。</div>}

      {topic === "quality" && extra && (
        <div className="mb-2 flex items-baseline gap-2">
          <span className="text-2xl font-bold text-brand-600">{(extra.overall * 100).toFixed(0)}</span>
          <span className="text-[12px] text-gray-400">综合质量分 / 100</span>
        </div>
      )}

      {rows && rows.length > 0 && (
        <div className="space-y-1.5">
          {topic === "segment" && rows.map((r, i) => (
            <Row key={i} left={r.segment_name || r.segment_code}
                 mid={`触达 ${r.sent_count ?? 0} · 打开 ${pct(r.open_rate)} · 转化 ${pct(r.conversion_rate)}`}
                 right={<Score v={r.quality_score} />} />
          ))}
          {topic === "tag" && rows.map((r, i) => (
            <Row key={i} left={r.tag_name || r.tag_code}
                 mid={`权重 ${Number(r.weight ?? 1).toFixed(2)} · 圈选 ${r.select_count ?? 0} 次 · 转化 ${pct(r.avg_conversion_rate)}`}
                 right={<span className={`rounded px-1.5 py-0.5 text-[10px] ${r.status === "archived" ? "bg-gray-100 text-gray-500" : "bg-brand-50 text-brand-600"}`}>{r.status === "archived" ? "已归档" : "活跃"}</span>} />
          ))}
          {topic === "field" && rows.map((r, i) => (
            <Row key={i} left={r.field_name}
                 mid={`填充率 ${pct(r.fill_rate)} · 被用 ${r.usage_count ?? 0} 次`}
                 right={<span className={`rounded px-1.5 py-0.5 text-[10px] ${REC_BADGE[r.recommendation] || ""}`}>{REC_LABEL[r.recommendation] || r.recommendation}</span>} />
          ))}
          {topic === "quality" && rows.map((r, i) => (
            <Row key={i} left={`${r.object_type}.${r.field_name || "—"}`}
                 mid={`${r.check_type} · 异常 ${r.bad_rows}/${r.total_rows} 行`}
                 right={<span className={`rounded px-1.5 py-0.5 text-[10px] ${SEV_BADGE[r.severity] || ""}`}>{(Number(r.score) * 100).toFixed(0)}分</span>} />
          ))}
          {topic === "insight" && rows.map((r, i) => (
            <Row key={i} left={r.finding_type === "sku_drop" ? "SKU 断崖" : "客群漂移"}
                 mid={`${r.dimension} · ${r.metric} ${Number(r.change_pct) > 0 ? "+" : ""}${Number(r.change_pct).toFixed(0)}%`}
                 right={<span className={`rounded px-1.5 py-0.5 text-[10px] ${SEV_BADGE[r.severity] || ""}`}>{r.severity}</span>} />
          ))}
        </div>
      )}
    </CardShell>
  );
}

function Row({ left, mid, right }: { left: React.ReactNode; mid: React.ReactNode; right?: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 rounded-lg bg-gray-50/70 px-2.5 py-1.5">
      <span className="w-28 shrink-0 truncate text-[12px] font-medium text-gray-800">{left}</span>
      <span className="min-w-0 flex-1 truncate text-[11px] text-gray-500">{mid}</span>
      {right}
    </div>
  );
}

function Score({ v }: { v: any }) {
  if (v == null) return <span className="text-[11px] text-gray-300">—</span>;
  const n = Number(v);
  const cls = n >= 0.7 ? "text-green-600" : n >= 0.4 ? "text-amber-600" : "text-red-500";
  return <span className={`text-[12px] font-semibold ${cls}`}>{(n * 100).toFixed(0)}分</span>;
}
