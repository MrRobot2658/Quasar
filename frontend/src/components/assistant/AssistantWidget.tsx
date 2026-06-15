import { useEffect, useRef, useState } from "react";
import { Sparkles, X, Send, ArrowRight, Trash2 } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import { Spinner } from "../ui";
import { useLang } from "../../context/LangContext";
import { useTenant } from "../../context/TenantContext";
import { useAuth } from "../../context/AuthContext";
import {
  chatAssistant,
  listAssistantTasks,
  getAssistantHistory,
  clearAssistantHistory,
  type ChatMessage,
  type ChatStep,
  type ChatTask,
  type ChatCreated,
} from "../../api/assistant";

interface UiMessage {
  role: "user" | "assistant";
  content: string;
  steps?: ChatStep[];
  task?: ChatTask | null;
  agentName?: string;
  created?: ChatCreated | null;
}

export default function AssistantWidget() {
  const { tr } = useLang();
  const { tenant } = useTenant();
  const { user } = useAuth();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<UiMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [loadedFor, setLoadedFor] = useState<string | null>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // 首次打开（或切换用户/租户）时，按用户加载历史聊天记录
  useEffect(() => {
    if (!open || !user) return;
    const key = `${user.id}:${tenant}`;
    if (loadedFor === key) return;
    getAssistantHistory(user.id, tenant)
      .then((hist) => setMessages(hist.map((m) => ({ role: m.role, content: m.content }))))
      .catch(() => {})
      .finally(() => setLoadedFor(key));
  }, [open, user, tenant, loadedFor]);

  async function clearHistory() {
    if (!user) return;
    if (!window.confirm(tr("清空与智能助手的聊天记录？", "Clear your assistant chat history?"))) return;
    try { await clearAssistantHistory(user.id, tenant); } catch { /* ignore */ }
    setMessages([]);
  }

  // 新消息滚到底部
  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, loading]);

  // 轮询任务状态：最多 ~10 次，每 2s 更新某条 assistant 消息里的 task.status
  function pollTask(run_id: string) {
    let tries = 0;
    const timer = setInterval(async () => {
      tries += 1;
      try {
        const { tasks } = await listAssistantTasks();
        const found = tasks.find((t) => t.run_id === run_id);
        if (found) {
          setMessages((prev) =>
            prev.map((m) =>
              m.task && m.task.run_id === run_id
                ? { ...m, task: { ...m.task, status: found.status } }
                : m,
            ),
          );
          if (found.status === "success" || found.status === "failed") {
            clearInterval(timer);
            return;
          }
        }
      } catch {
        /* 忽略轮询错误 */
      }
      if (tries >= 10) clearInterval(timer);
    }, 2000);
  }

  async function send() {
    const text = input.trim();
    if (!text || loading) return;
    const userMsg: UiMessage = { role: "user", content: text };
    const history = [...messages, userMsg];
    setMessages(history);
    setInput("");
    setLoading(true);
    try {
      const payload: ChatMessage[] = history.map((m) => ({ role: m.role, content: m.content }));
      const res = await chatAssistant(tenant, payload, user?.id);
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: res.reply, steps: res.steps, task: res.task, agentName: res.agent_name, created: res.created },
      ]);
      if (res.task?.run_id) pollTask(res.task.run_id);
      // 助手要求「打开页面」：跳转并收起面板
      if (res.navigate?.path) {
        const path = res.navigate.path;
        setTimeout(() => { setOpen(false); navigate(path); }, 400);
      }
    } catch (e: any) {
      const errText = e?.response?.data?.detail || e?.message || tr("请求失败", "Request failed");
      setMessages((prev) => [...prev, { role: "assistant", content: String(errText) }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="inline-flex items-center gap-1.5 rounded-lg bg-brand-50 px-3 py-1.5 text-sm font-medium text-brand-700 transition-colors hover:bg-brand-100"
      >
        <Sparkles className="h-4 w-4" />
        {tr("智能助手", "Assistant")}
      </button>

      {open && (
        <div className="fixed right-4 top-16 z-50 flex h-[70vh] w-96 max-w-[calc(100vw-2rem)] flex-col rounded-2xl border border-gray-200 bg-white shadow-xl">
          {/* 头部 */}
          <div className="flex items-center justify-between border-b border-gray-200 px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-semibold text-gray-900">
              <Sparkles className="h-4 w-4 text-brand-600" />
              {tr("智能助手", "Assistant")}
            </div>
            <div className="flex items-center gap-1">
              {messages.length > 0 && (
                <button
                  type="button"
                  onClick={clearHistory}
                  className="rounded-md p-1.5 text-gray-400 hover:bg-red-50 hover:text-red-600"
                  title={tr("清空记录", "Clear history")}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
              <button
                type="button"
                onClick={() => setOpen(false)}
                className="rounded-md p-1 text-gray-400 hover:bg-gray-100 hover:text-gray-600"
                aria-label={tr("关闭", "Close")}
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* 消息列表 */}
          <div ref={listRef} className="flex-1 space-y-3 overflow-y-auto px-4 py-4">
            {messages.length === 0 && !loading && (
              <div className="px-2 py-8 text-center text-sm text-gray-400">
                {tr(
                  "问我任何关于数据底座的问题，或让我发布一个后台任务。",
                  "Ask me anything about the data foundation, or have me publish a background task.",
                )}
              </div>
            )}
            {messages.map((m, i) => (
              <div key={i} className={m.role === "user" ? "flex justify-end" : "flex justify-start"}>
                <div
                  className={`max-w-[85%] rounded-2xl px-3 py-2 text-sm ${
                    m.role === "user"
                      ? "bg-brand-500 text-white"
                      : "bg-gray-100 text-gray-800"
                  }`}
                >
                  {m.role === "assistant" && m.agentName && (
                    <div className="mb-1 inline-flex items-center rounded-full bg-brand-100 px-2 py-0.5 text-[10px] font-semibold text-brand-700">
                      {tr("智能体", "Agent")} · {m.agentName}
                    </div>
                  )}
                  <div className="whitespace-pre-wrap break-words">{m.content}</div>
                  {m.created && (
                    <Link
                      to={m.created.path}
                      onClick={() => setOpen(false)}
                      className="mt-2 inline-flex items-center gap-1 rounded-lg bg-brand-50 px-2 py-1 text-[11px] font-medium text-brand-700 ring-1 ring-inset ring-brand-200 hover:bg-brand-100"
                    >
                      {m.created.kind === "dashboard" ? tr("打开看板", "Open dashboard") : tr("打开分析", "Open analytics")} · {m.created.title}
                      <ArrowRight className="h-3 w-3" />
                    </Link>
                  )}
                  {m.steps && m.steps.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {m.steps.map((s, j) => (
                        <span
                          key={j}
                          className="inline-flex items-center rounded-full bg-white/70 px-2 py-0.5 text-[11px] font-medium text-gray-500 ring-1 ring-inset ring-gray-200"
                          title={s.summary}
                        >
                          🔧 {s.tool}
                        </span>
                      ))}
                    </div>
                  )}
                  {m.task && (
                    <div className="mt-2 inline-flex flex-wrap items-center gap-1 rounded-lg bg-amber-50 px-2 py-1 text-[11px] font-medium text-amber-700 ring-1 ring-inset ring-amber-200">
                      {tr("任务已发布", "Task published")} · {m.task.task_name} · {m.task.run_id} · {m.task.status}
                    </div>
                  )}
                </div>
              </div>
            ))}
            {loading && (
              <div className="flex justify-start">
                <div className="flex items-center gap-2 rounded-2xl bg-gray-100 px-3 py-2 text-sm text-gray-500">
                  <Spinner /> {tr("思考中…", "Thinking…")}
                </div>
              </div>
            )}
          </div>

          {/* 输入区 */}
          <div className="border-t border-gray-200 p-3">
            <div className="flex items-end gap-2">
              <textarea
                rows={1}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    send();
                  }
                }}
                placeholder={tr("输入消息…", "Type a message…")}
                className="max-h-32 min-h-[38px] flex-1 resize-none rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
              />
              <button
                type="button"
                onClick={send}
                disabled={loading || !input.trim()}
                className="inline-flex h-[38px] items-center justify-center gap-1 rounded-lg bg-brand-500 px-3 text-sm font-medium text-white transition-colors hover:bg-brand-600 disabled:cursor-not-allowed disabled:bg-brand-300"
              >
                <Send className="h-4 w-4" />
                {tr("发送", "Send")}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
