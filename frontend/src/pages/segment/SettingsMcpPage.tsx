import { useEffect, useState } from "react";
import Layout from "../../components/layout/Layout";
import { Card, DataTable, Spinner } from "../../components/ui";
import { SubTabs } from "../../components/segment/kit";
import { useLang } from "../../context/LangContext";
import { getMcpTools, type McpToolsResponse } from "../../api/assistant";

export default function SettingsMcpPage() {
  const { tr } = useLang();

  const TABS = [
    { label: tr("通用", "General"), to: "/settings" },
    { label: tr("权限管理", "Access"), to: "/settings/access" },
    { label: tr("API 令牌", "API Tokens"), to: "/settings/tokens" },
    { label: tr("审计日志", "Audit Log"), to: "/settings/audit" },
    { label: tr("MCP 设置", "MCP"), to: "/settings/mcp" },
  ];

  const COL = {
    tool: tr("工具", "Tool"),
    desc: tr("说明", "Description"),
    params: tr("参数", "Params"),
  };

  const [data, setData] = useState<McpToolsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    getMcpTools()
      .then(setData)
      .catch((e: any) =>
        setError(e?.response?.data?.detail || e?.message || tr("加载失败", "Failed to load")),
      )
      .finally(() => setLoading(false));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const tools = data?.tools || [];

  return (
    <Layout
      title={tr("MCP 设置", "MCP Settings")}
      subtitle={tr(
        "智能助手可调用的 MCP 工具（只读）",
        "Read-only MCP tools the assistant can call",
      )}
    >
      <SubTabs tabs={TABS.map((t) => ({ ...t, active: t.to === "/settings/mcp" }))} />

      {error && <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{error}</div>}

      {loading ? (
        <div className="flex justify-center py-12"><Spinner /></div>
      ) : (
        <>
          {data?.server && (
            <Card className="mb-4 p-4">
              <div className="flex flex-wrap gap-x-8 gap-y-2 text-sm">
                <div>
                  <span className="text-gray-400">{tr("服务", "Server")}：</span>
                  <span className="font-medium text-gray-900">{data.server.name}</span>
                </div>
                <div>
                  <span className="text-gray-400">{tr("传输", "Transport")}：</span>
                  <span className="font-medium text-gray-900">{data.server.transport}</span>
                </div>
                <div>
                  <span className="text-gray-400">{tr("路径", "Path")}：</span>
                  <code className="rounded bg-gray-100 px-1.5 py-0.5 text-xs">{data.server.path}</code>
                </div>
              </div>
            </Card>
          )}

          {data?.error && (
            <div className="mb-4 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-600">{data.error}</div>
          )}

          <Card className="p-2">
            <div className="px-3 pb-2 pt-3 text-sm font-semibold text-gray-700">
              {tr(`共 ${tools.length} 个工具`, `${tools.length} tools`)}
            </div>
            <DataTable
              columns={[COL.tool, COL.desc, COL.params]}
              rows={tools.map((t) => {
                const keys = Object.keys(t.parameters?.properties || {});
                return {
                  [COL.tool]: <code className="text-xs font-semibold text-brand-700">{t.name}</code>,
                  [COL.desc]: <span className="text-gray-600">{t.description || "—"}</span>,
                  [COL.params]: keys.length ? (
                    <span className="text-xs text-gray-500">{keys.join(", ")}</span>
                  ) : "—",
                };
              })}
            />
            {tools.length === 0 && !data?.error && (
              <div className="px-6 py-12 text-center text-sm text-gray-500">
                {tr("暂无可用工具", "No tools available")}
              </div>
            )}
          </Card>
        </>
      )}
    </Layout>
  );
}
