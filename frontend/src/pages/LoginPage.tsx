import { useState, type FormEvent } from "react";
import { Navigate, useNavigate } from "react-router-dom";
import { Button } from "../components/ui";
import { useAuth } from "../context/AuthContext";
import { useLang } from "../context/LangContext";

export default function LoginPage() {
  const { user, login } = useAuth();
  const { tr } = useLang();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  if (user) return <Navigate to="/" replace />;

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await login(email, password);
      navigate("/");
    } catch (err: any) {
      if (err?.response?.status === 401) {
        setError(tr("邮箱或密码错误", "Invalid email or password"));
      } else {
        setError(err?.response?.data?.detail || err?.message || tr("登录失败", "Login failed"));
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-gray-50 px-4">
      <div className="w-full max-w-sm">
        <div className="mb-6 text-center">
          <h1 className="text-2xl font-bold text-gray-900">Quasar</h1>
          <p className="mt-1 text-sm text-gray-500">
            {tr("多数据源计算存储智能代理 · 为 Agent 提供数据底座", "Multi-source compute & storage agent · the data foundation for AI Agents")}
          </p>
        </div>

        <form
          onSubmit={onSubmit}
          className="rounded-2xl border border-gray-200 bg-white p-6 shadow-card"
        >
          <label className="mb-4 block">
            <span className="mb-1 block text-sm font-medium text-gray-700">
              {tr("邮箱", "Email")}
            </span>
            <input
              type="email"
              autoComplete="username"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="admin@acme.com"
            />
          </label>

          <label className="mb-4 block">
            <span className="mb-1 block text-sm font-medium text-gray-700">
              {tr("密码", "Password")}
            </span>
            <input
              type="password"
              autoComplete="current-password"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-brand-400 focus:outline-none"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
            />
          </label>

          {error && (
            <div className="mb-4 rounded-lg bg-red-50 px-3 py-2 text-sm text-red-600">{error}</div>
          )}

          <Button type="submit" disabled={submitting} className="w-full">
            {submitting ? tr("登录中…", "Signing in…") : tr("登录", "Login")}
          </Button>

          <div className="mt-4 rounded-lg bg-gray-50 px-3 py-2 text-xs text-gray-500">
            {tr("演示账号", "Demo")}: admin@acme.com · {tr("密码", "password")} demo123
          </div>
        </form>
      </div>
    </div>
  );
}
