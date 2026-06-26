import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  Sparkles, Send, Plus, Trash2, Paperclip, X, ChevronDown, LogOut, Bot, MessageCircleQuestion,
  UserSearch, Filter, BarChart3, MapPin, Settings, type LucideIcon,
} from "lucide-react";
import { Spinner } from "../ui";
import Markdown from "../assistant/Markdown";
import ViewCard from "./cards/ViewCard";
import SettingsPanel from "./SettingsPanel";
import WelcomeGreeting from "./WelcomeGreeting";
import { ChatActionCtx } from "../../context/ChatActionContext";
import { getMemory, addTokens } from "../../lib/prefs";
import { useLang, type Lang } from "../../context/LangContext";
import { useTenant } from "../../context/TenantContext";
import { useAuth } from "../../context/AuthContext";
import {
  chatAssistant, listConversations, getAssistantHistory, clearAssistantHistory, listAssistantTasks,
  type ChatMessage, type ChatStep, type ChatTask, type ChatCreated, type ChatView, type ChatMode, type Conversation,
} from "../../api/assistant";

interface Attachment { name: string; content: string }
interface UiMessage {
  role: "user" | "assistant";
  content: string;
  steps?: ChatStep[];
  task?: ChatTask | null;
  agentName?: string;
  created?: ChatCreated | null;
  views?: ChatView[];
}

const newId = () => (crypto?.randomUUID?.() ? `c_${crypto.randomUUID().slice(0, 8)}` : `c_${Math.random().toString(36).slice(2, 10)}`);

// 常见任务：新会话首页展示，点击即作为提问发送
interface CommonTask { icon: LucideIcon; title: string; titleEn: string; prompt: string }
const COMMON_TASKS: CommonTask[] = [
  { icon: UserSearch, title: "查看用户画像", titleEn: "Inspect a profile", prompt: "看 OneID 100002 的用户画像" },
  { icon: Filter, title: "圈选高价值人群", titleEn: "Build an audience", prompt: "找近 30 天在抖音点过广告、且下过单的高价值用户" },
  { icon: BarChart3, title: "渠道分布图表", titleEn: "Channel breakdown chart", prompt: "按渠道统计用户分布做个柱状图" },
  { icon: MapPin, title: "查询线索", titleEn: "Query leads", prompt: "列出北京的线索" },
];
const TEXT_ACCEPT = ".csv,.tsv,.txt,.json,.md,.log,.yaml,.yml";

export default function ChatApp() {
  const { tr, lang, setLang } = useLang();
  const { tenant, setTenant, tenants } = useTenant();
  const { user, logout } = useAuth();

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [convId, setConvId] = useState<string>(newId());
  const [messages, setMessages] = useState<UiMessage[]>([]);
  const [input, setInput] = useState("");
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [mode, setMode] = useState<ChatMode>("agent");
  const [loading, setLoading] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const navigate = useNavigate();
  const loc = useLocation();
  const listRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  const refreshConversations = useCallback(() => {
    if (!user) return;
    listConversations(user.id, tenant).then(setConversations).catch(() => {});
  }, [user, tenant]);

  useEffect(() => { refreshConversations(); }, [refreshConversations]);
  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, loading]);

  function newChat() {
    setConvId(newId());
    setMessages([]);
    setAttachments([]);
    setInput("");
    if (loc.pathname !== "/") navigate("/");   // 清空页面：关闭已打开的功能页，回到常见任务首页
    setTimeout(() => inputRef.current?.focus(), 30);
  }

  async function openConversation(id: string) {
    if (id === convId) return;
    setConvId(id);
    setMessages([]);
    if (!user) return;
    try {
      const hist = await getAssistantHistory(user.id, tenant, id);
      setMessages(hist.map((m) => ({ role: m.role, content: m.content }) as UiMessage));
    } catch { /* ignore */ }
  }

  async function deleteConversation(id: string, e: React.MouseEvent) {
    e.stopPropagation();
    if (!user || !window.confirm(tr("删除该会话？", "Delete this conversation?"))) return;
    try { await clearAssistantHistory(user.id, tenant, id); } catch { /* ignore */ }
    if (id === convId) newChat();
    refreshConversations();
  }

  function onFiles(files: FileList | null) {
    if (!files) return;
    Array.from(files).forEach((f) => {
      const reader = new FileReader();
      reader.onload = () => setAttachments((a) => [...a, { name: f.name, content: String(reader.result || "").slice(0, 8000) }]);
      reader.readAsText(f);
    });
  }

  function pollTask(run_id: string) {
    let tries = 0;
    const timer = setInterval(async () => {
      tries += 1;
      try {
        const { tasks } = await listAssistantTasks();
        const found = tasks.find((t) => t.run_id === run_id);
        if (found) {
          setMessages((prev) => prev.map((m) => (m.task && m.task.run_id === run_id ? { ...m, task: { ...m.task, status: found.status } } : m)));
          if (found.status === "success" || found.status === "failed") { clearInterval(timer); return; }
        }
      } catch { /* ignore */ }
      if (tries >= 10) clearInterval(timer);
    }, 2000);
  }

  const send = useCallback(
    async (text?: string) => {
      const typed = (text ?? input).trim();
      if ((!typed && attachments.length === 0) || loading) return;
      const atts = attachments;
      // 折叠附件正文进入发送内容；气泡展示仍以用户输入为主
      const sentContent = atts.length
        ? `${typed}\n\n${atts.map((a) => `【附件：${a.name}】\n${a.content}`).join("\n\n")}`
        : typed;
      const displayContent = atts.length ? `${typed}${typed ? "\n" : ""}📎 ${atts.map((a) => a.name).join("、")}` : typed;

      const history = [...messages, { role: "user", content: displayContent } as UiMessage];
      setMessages(history);
      setInput("");
      setAttachments([]);
      setLoading(true);
      try {
        // 发给后端的历史用 sentContent 作为最后一条用户消息
        const payload: ChatMessage[] = [
          ...messages.map((m) => ({ role: m.role, content: m.content })),
          { role: "user", content: sentContent },
        ];
        // 注入长期记忆（设置面板）作为系统上下文
        const mem = getMemory();
        if (mem) payload.unshift({ role: "system", content: `【长期记忆】\n${mem}` });
        addTokens(sentContent);
        const res = await chatAssistant(tenant, payload, { user_id: user?.id, conversation_id: convId, mode });
        addTokens(res.reply);
        setMessages((prev) => [
          ...prev,
          { role: "assistant", content: res.reply, steps: res.steps, task: res.task, agentName: res.agent_name, created: res.created, views: res.views },
        ]);
        if (res.task?.run_id) pollTask(res.task.run_id);
        refreshConversations();
      } catch (e: any) {
        const errText = e?.response?.data?.detail || e?.message || tr("请求失败", "Request failed");
        setMessages((prev) => [...prev, { role: "assistant", content: String(errText) }]);
      } finally {
        setLoading(false);
      }
    },
    [input, attachments, loading, messages, tenant, user, convId, mode, tr, refreshConversations],
  );

  const langOpts: { key: Lang; label: string }[] = [{ key: "zh", label: "中" }, { key: "en", label: "EN" }];
  const initial = (user?.name || user?.email || "A").charAt(0).toUpperCase();

  return (
    <ChatActionCtx.Provider value={{ ask: (t) => { send(t); } }}>
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* 会话列表 */}
      <aside className="hidden w-60 shrink-0 flex-col border-r border-gray-200 bg-white md:flex">
        <div className="flex items-center gap-2 px-4 py-3.5">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-brand-400 text-white">
            <Sparkles className="h-4 w-4" />
          </div>
          <span className="text-sm font-bold tracking-tight text-gray-900">Quasar</span>
        </div>
        <div className="px-3">
          <button
            type="button"
            onClick={newChat}
            className="flex w-full items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm font-medium text-gray-700 hover:border-brand-300 hover:bg-brand-50"
          >
            <Plus className="h-4 w-4" /> {tr("新会话", "New chat")}
          </button>
        </div>
        <div className="mt-2 flex-1 space-y-1 overflow-y-auto px-2 py-1">
          {/* 会话 */}
          <div className="px-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400">{tr("会话", "Chats")}</div>
          {conversations.length === 0 && <div className="px-3 py-3 text-center text-xs text-gray-400">{tr("暂无历史会话", "No conversations yet")}</div>}
          {conversations.map((c) => (
            <button
              key={c.conversation_id}
              type="button"
              onClick={() => openConversation(c.conversation_id)}
              className={`group flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[13px] ${
                c.conversation_id === convId ? "bg-brand-50 text-brand-700" : "text-gray-600 hover:bg-gray-100"
              }`}
            >
              <span className="flex-1 truncate">{c.title || tr("新会话", "New chat")}</span>
              <Trash2
                className="h-3.5 w-3.5 shrink-0 text-gray-300 opacity-0 hover:text-red-500 group-hover:opacity-100"
                onClick={(e) => deleteConversation(c.conversation_id, e)}
              />
            </button>
          ))}
        </div>
        {/* 底部：设置（记忆 / 技能 / MCP / 数据源 / 知识库 / Token）*/}
        <div className="border-t border-gray-200 p-3">
          <button type="button" onClick={() => setSettingsOpen(true)}
            className="flex w-full items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-[13px] font-medium text-gray-600 hover:border-brand-300 hover:bg-brand-50">
            <Settings className="h-4 w-4" /> {tr("设置", "Settings")}
          </button>
        </div>
      </aside>

      {/* 右列：TailAdmin 顶栏 + 对话主区 */}
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        {/* 顶栏：左=管理员登录信息，右=租户 logo + 语言 */}
        <header className="flex items-center gap-3 border-b border-gray-200 bg-white px-5 py-2.5">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-full bg-brand-100 text-xs font-semibold text-brand-700">{initial}</div>
            <div className="leading-tight">
              <div className="text-[13px] font-semibold text-gray-900">{user?.name || tr("管理员", "Admin")}</div>
              <div className="text-[11px] text-gray-400">{user?.email}</div>
            </div>
            <button type="button" onClick={() => { logout(); location.href = "/login"; }}
              className="ml-1 rounded-md p-1 text-gray-400 hover:bg-red-50 hover:text-red-600" title={tr("退出登录", "Log out")}>
              <LogOut className="h-4 w-4" />
            </button>
          </div>
          <div className="flex-1" />
          <div className="flex items-center rounded-md border border-gray-200 p-0.5">
            {langOpts.map((o) => (
              <button key={o.key} type="button" onClick={() => setLang(o.key)}
                className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${lang === o.key ? "bg-brand-50 text-brand-700" : "text-gray-400 hover:text-gray-700"}`}>
                {o.label}
              </button>
            ))}
          </div>
          {/* 租户 logo（右上角）*/}
          <div className="flex items-center gap-2 rounded-lg border border-gray-200 bg-gray-50 px-2.5 py-1.5">
            <div className="flex h-7 w-7 items-center justify-center rounded-md bg-gradient-to-br from-brand-500 to-indigo-500 text-[12px] font-bold text-white">{String(tenant).slice(0, 2)}</div>
            <div className="relative">
              <select value={tenant} onChange={(e) => setTenant(Number(e.target.value))}
                className="appearance-none bg-transparent pr-5 text-[12px] font-semibold text-gray-700 focus:outline-none" title="Workspace">
                {tenants.map((tid) => <option key={tid} value={tid}>{tr("租户", "Tenant")} {tid}</option>)}
              </select>
              <ChevronDown className="pointer-events-none absolute right-0 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-gray-400" />
            </div>
          </div>
        </header>

      {/* 对话主区 */}
      <main className="flex min-h-0 min-w-0 flex-1 flex-col">
        <div ref={listRef} className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-3xl space-y-5 px-4 py-5">
            {/* 新会话首页：清空页面，仅展示常见任务（无消息时）*/}
            {messages.length === 0 && !loading && (
              <div className="flex min-h-[60vh] flex-col items-center justify-center py-8">
                <div className="mb-3 flex h-12 w-12 items-center justify-center rounded-2xl bg-brand-50 text-brand-500">
                  <Sparkles className="h-6 w-6" />
                </div>
                <WelcomeGreeting name={user?.name || user?.email || tr("朋友", "friend")} />
                <div className="mt-6 grid w-full max-w-xl grid-cols-1 gap-2.5 sm:grid-cols-2">
                  {COMMON_TASKS.map((t) => (
                    <button key={t.title} type="button" onClick={() => send(t.prompt)}
                      className="group flex items-start gap-3 rounded-xl border border-gray-200 bg-white px-3.5 py-3 text-left transition-colors hover:border-brand-300 hover:bg-brand-50/40">
                      <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-gray-100 text-gray-500 group-hover:bg-brand-100 group-hover:text-brand-600">
                        <t.icon className="h-[18px] w-[18px]" />
                      </span>
                      <span className="min-w-0">
                        <span className="block text-[13px] font-semibold text-gray-900">{tr(t.title, t.titleEn)}</span>
                        <span className="mt-0.5 block truncate text-[12px] text-gray-400">{t.prompt}</span>
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* 对话消息 */}
            <div className="space-y-5">
                {messages.map((m, i) => (
                  <div key={i} className={m.role === "user" ? "flex justify-end" : "flex flex-col items-start"}>
                    <div className={`max-w-[90%] rounded-2xl px-3.5 py-2.5 text-sm ${m.role === "user" ? "bg-brand-500 text-white" : "bg-white text-gray-800 ring-1 ring-gray-200"}`}>
                      {m.role === "assistant" && m.agentName && (
                        <div className="mb-1 inline-flex items-center gap-1 rounded-full bg-brand-100 px-2 py-0.5 text-[10px] font-semibold text-brand-700">
                          <Bot className="h-3 w-3" /> {m.agentName}
                        </div>
                      )}
                      {m.role === "assistant" ? <Markdown>{m.content}</Markdown> : <div className="whitespace-pre-wrap break-words">{m.content}</div>}
                      {m.steps && m.steps.length > 0 && (
                        <div className="mt-2 flex flex-wrap gap-1">
                          {m.steps.map((s, j) => (
                            <span key={j} className="inline-flex items-center rounded-full bg-gray-50 px-2 py-0.5 text-[11px] font-medium text-gray-500 ring-1 ring-inset ring-gray-200" title={s.summary}>🔧 {s.tool}</span>
                          ))}
                        </div>
                      )}
                      {m.task && (
                        <div className="mt-2 inline-flex flex-wrap items-center gap-1 rounded-lg bg-amber-50 px-2 py-1 text-[11px] font-medium text-amber-700 ring-1 ring-inset ring-amber-200">
                          {tr("任务已发布", "Task published")} · {m.task.task_name} · {m.task.status}
                        </div>
                      )}
                    </div>
                    {/* 内联卡片：渲染 agent 返回的 views */}
                    {m.role === "assistant" && m.views && m.views.length > 0 && (
                      <div className="w-full max-w-[90%] space-y-2">
                        {m.views.map((v, k) => <ViewCard key={k} view={v} />)}
                      </div>
                    )}
                  </div>
                ))}
                {loading && (
                  <div className="flex items-center gap-2 text-sm text-gray-400"><Spinner /> {tr("思考中…", "Thinking…")}</div>
                )}
            </div>
          </div>
        </div>

        {/* 输入区 */}
        <div className="shrink-0 border-t border-gray-200 bg-white px-4 py-3">
          <div className="mx-auto max-w-3xl">
            {attachments.length > 0 && (
              <div className="mb-2 flex flex-wrap gap-1.5">
                {attachments.map((a, i) => (
                  <span key={i} className="inline-flex items-center gap-1 rounded-lg bg-gray-100 px-2 py-1 text-[11px] text-gray-600">
                    <Paperclip className="h-3 w-3" /> {a.name}
                    <X className="h-3 w-3 cursor-pointer hover:text-red-500" onClick={() => setAttachments((arr) => arr.filter((_, j) => j !== i))} />
                  </span>
                ))}
              </div>
            )}
            <div className="flex items-end gap-2 rounded-2xl border border-gray-300 bg-white px-2 py-1.5 focus-within:border-brand-400">
              {/* ask / agent 切换 */}
              <div className="flex items-center rounded-lg bg-gray-100 p-0.5">
                <button type="button" onClick={() => setMode("ask")} title={tr("提问：只回答与解释", "Ask: answer & explain only")}
                  className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-[12px] font-medium ${mode === "ask" ? "bg-white text-brand-700 shadow-sm" : "text-gray-400"}`}>
                  <MessageCircleQuestion className="h-3.5 w-3.5" /> Ask
                </button>
                <button type="button" onClick={() => setMode("agent")} title={tr("智能体：执行操作并渲染卡片", "Agent: take actions & render cards")}
                  className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-[12px] font-medium ${mode === "agent" ? "bg-white text-brand-700 shadow-sm" : "text-gray-400"}`}>
                  <Bot className="h-3.5 w-3.5" /> Agent
                </button>
              </div>
              <button type="button" onClick={() => fileRef.current?.click()} className="rounded-lg p-2 text-gray-400 hover:bg-gray-100 hover:text-gray-600" title={tr("上传附件", "Attach files")}>
                <Paperclip className="h-4 w-4" />
              </button>
              <input ref={fileRef} type="file" multiple accept={TEXT_ACCEPT} className="hidden" onChange={(e) => { onFiles(e.target.files); e.target.value = ""; }} />
              <textarea
                ref={inputRef}
                rows={1}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
                placeholder={mode === "ask" ? tr("提问任何关于数据底座的问题…", "Ask anything about the data foundation…") : tr("让我帮你圈人群、查画像、建图表…", "Ask me to build audiences, inspect profiles, create charts…")}
                className="max-h-40 min-h-[36px] flex-1 resize-none bg-transparent px-1 py-1.5 text-sm focus:outline-none"
              />
              <button type="button" onClick={() => send()} disabled={loading || (!input.trim() && attachments.length === 0)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-xl bg-brand-500 text-white transition-colors hover:bg-brand-600 disabled:cursor-not-allowed disabled:bg-brand-300">
                {loading ? <Spinner /> : <Send className="h-4 w-4" />}
              </button>
            </div>
          </div>
        </div>
      </main>
      </div>

      {/* 设置弹层（含 数据源 / 知识库 / 记忆 / 技能 / MCP / Token）*/}
      <SettingsPanel open={settingsOpen} onClose={() => setSettingsOpen(false)} />
    </div>
    </ChatActionCtx.Provider>
  );
}
