import { createContext, useContext, useState, type ReactNode } from "react";

interface AssistantCtx {
  open: boolean;
  setOpen: (o: boolean) => void;
  toggle: () => void;
}

const Ctx = createContext<AssistantCtx>({ open: false, setOpen: () => {}, toggle: () => {} });
const KEY = "cdp_assistant_open";

export function AssistantProvider({ children }: { children: ReactNode }) {
  const [open, setOpenState] = useState<boolean>(() => {
    try { return localStorage.getItem(KEY) === "1"; } catch { return false; }
  });
  const setOpen = (o: boolean) => {
    setOpenState(o);
    try { localStorage.setItem(KEY, o ? "1" : "0"); } catch { /* ignore */ }
  };
  return (
    <Ctx.Provider value={{ open, setOpen, toggle: () => setOpen(!open) }}>
      {children}
    </Ctx.Provider>
  );
}

export const useAssistant = () => useContext(Ctx);
