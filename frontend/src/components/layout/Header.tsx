import { ChevronDown, Search } from "lucide-react";
import { useTenant } from "../../context/TenantContext";
import { useLang, type Lang } from "../../context/LangContext";
import AssistantWidget from "../assistant/AssistantWidget";

// 语言切换：中 / EN 分段开关
function LangSwitch() {
  const { lang, setLang } = useLang();
  const opts: { key: Lang; label: string }[] = [
    { key: "zh", label: "中" },
    { key: "en", label: "EN" },
  ];
  return (
    <div className="flex items-center rounded-lg border border-gray-200 p-0.5" role="group" aria-label="Language">
      {opts.map((o) => (
        <button
          key={o.key}
          type="button"
          onClick={() => setLang(o.key)}
          className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
            lang === o.key ? "bg-brand-50 text-brand-700" : "text-gray-500 hover:text-gray-800"
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export default function Header() {
  const { tenant, setTenant, tenants } = useTenant();
  const { t } = useLang();
  return (
    <header className="sticky top-0 z-10 flex h-14 items-center justify-between border-b border-gray-200 bg-white px-6">
      <div className="flex items-center gap-2 text-sm text-gray-400">
        <Search className="h-4 w-4" />
        <span className="hidden sm:inline">{t("search")}</span>
      </div>
      <div className="flex items-center gap-3">
        <AssistantWidget />
        <LangSwitch />
        <div className="relative">
          <select
            value={tenant}
            onChange={(e) => setTenant(Number(e.target.value))}
            className="appearance-none rounded-lg border border-gray-200 bg-white py-1.5 pl-3 pr-8 text-sm font-medium text-gray-700 focus:border-brand-400 focus:outline-none"
            title="Workspace"
          >
            {tenants.map((tid) => (
              <option key={tid} value={tid}>Workspace · {t("tenant")} {tid}</option>
            ))}
          </select>
          <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
        </div>
        <div className="flex h-8 w-8 items-center justify-center rounded-full bg-brand-100 text-sm font-semibold text-brand-700">
          A
        </div>
      </div>
    </header>
  );
}
