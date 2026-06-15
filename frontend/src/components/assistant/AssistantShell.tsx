import { type ReactNode } from "react";
import AssistantWidget from "./AssistantWidget";
import { useAssistant } from "../../context/AssistantContext";

// 应用外壳：智能助手以右侧停靠侧边栏呈现；打开时主内容右移让位（lg+），互不遮挡。
export default function AssistantShell({ children }: { children: ReactNode }) {
  const { open } = useAssistant();
  return (
    <>
      <div className={`transition-[margin] duration-300 ease-out ${open ? "lg:mr-[400px]" : ""}`}>
        {children}
      </div>
      <AssistantWidget />
    </>
  );
}
