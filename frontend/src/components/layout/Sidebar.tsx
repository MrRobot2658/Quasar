import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { ChevronDown } from "lucide-react";
import { HOME, SECTIONS, FOOTER_SECTION, type NavChild, type NavSection } from "../../lib/nav";
import { useLang } from "../../context/LangContext";

const rowBase =
  "flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors";
const active = "bg-brand-50 text-brand-700 font-semibold";
const idle = "text-gray-600 hover:bg-gray-100 hover:text-gray-900";

// 按当前语言取菜单文案：中文用 label，英文用 term（缺省回退 label）
function useNavText() {
  const { lang } = useLang();
  return (item: { label: string; term?: string }) =>
    lang === "en" ? (item.term ?? item.label) : item.label;
}

function TopItem({ item }: { item: NavChild }) {
  const txt = useNavText();
  return (
    <NavLink to={item.to} end className={({ isActive }) => `${rowBase} ${isActive ? active : idle}`}>
      <item.icon className="h-[18px] w-[18px] shrink-0" />
      <span className="flex-1">{txt(item)}</span>
    </NavLink>
  );
}

function ChildItem({ item }: { item: NavChild }) {
  const txt = useNavText();
  return (
    <NavLink
      to={item.to}
      end
      className={({ isActive }) =>
        `flex items-center gap-2.5 rounded-md py-1.5 pl-9 pr-3 text-[13px] transition-colors ${
          isActive ? "bg-brand-50 font-medium text-brand-700" : "text-gray-500 hover:bg-gray-100 hover:text-gray-800"
        }`
      }
    >
      <item.icon className="h-4 w-4 shrink-0" />
      <span className="flex-1">{txt(item)}</span>
    </NavLink>
  );
}

// 有子菜单：父级仅展开/收起，不跳转；落地页交给子菜单。
// 当路由进入本分区时自动展开；之后尊重用户的手动开合。
function CollapsibleSection({ section, sectionActive }: { section: NavSection; sectionActive: boolean }) {
  const [open, setOpen] = useState(sectionActive);
  const txt = useNavText();
  useEffect(() => {
    if (sectionActive) setOpen(true);
  }, [sectionActive]);

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={`${rowBase} w-full text-left ${sectionActive ? active : idle}`}
      >
        <section.icon className="h-[18px] w-[18px] shrink-0" />
        <span className="flex-1 font-medium">{txt(section)}</span>
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-gray-400 transition-transform ${open ? "rotate-180" : ""}`}
        />
      </button>
      {open && section.children && (
        <div className="mt-1 space-y-0.5">
          {section.children.map((c) => <ChildItem key={c.to} item={c} />)}
        </div>
      )}
    </div>
  );
}

function Section({ section, sectionActive }: { section: NavSection; sectionActive: boolean }) {
  // 无子菜单的分区（如「客户」）保持普通跳转链接；有子菜单的父级仅展开。
  return section.children
    ? <CollapsibleSection section={section} sectionActive={sectionActive} />
    : <TopItem item={section} />;
}

export default function Sidebar() {
  const { pathname } = useLocation();
  const { t } = useLang();
  return (
    <aside className="hidden w-64 shrink-0 border-r border-gray-200 bg-white lg:flex lg:flex-col">
      <div className="flex h-16 items-center gap-2.5 border-b border-gray-200 px-5">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-brand-400 text-white">
          <svg viewBox="0 0 24 24" className="h-5 w-5" fill="currentColor">
            <path d="M3 13.5h11.5a3.5 3.5 0 0 1 0 7H7a1 1 0 1 1 0-2h7.5a1.5 1.5 0 0 0 0-3H3a1 1 0 0 1 0-2Zm6.5-10H17a1 1 0 1 1 0 2H9.5a1.5 1.5 0 0 0 0 3H21a1 1 0 1 1 0 2H9.5a3.5 3.5 0 0 1 0-7Z" />
          </svg>
        </div>
        <div>
          <div className="text-sm font-bold tracking-tight text-gray-900">AgenticDMP</div>
          <div className="text-[11px] text-gray-400">{t("brandSub")}</div>
        </div>
      </div>

      <nav className="flex-1 space-y-1.5 overflow-y-auto px-3 py-4">
        <TopItem item={HOME} />
        <div className="!mt-3 mb-1 px-3 text-[10px] font-semibold uppercase tracking-wider text-gray-400">
          {t("workspace")}
        </div>
        {SECTIONS.map((s) => (
          <Section key={s.to} section={s} sectionActive={pathname === s.to || pathname.startsWith(s.to + "/")} />
        ))}
        <div className="!mt-3 border-t border-gray-200 pt-3">
          <Section
            section={FOOTER_SECTION}
            sectionActive={pathname === FOOTER_SECTION.to || pathname.startsWith(FOOTER_SECTION.to + "/")}
          />
        </div>
      </nav>

      <div className="border-t border-gray-200 px-5 py-3 text-[11px] text-gray-400">
        {t("footer")}
      </div>
    </aside>
  );
}
