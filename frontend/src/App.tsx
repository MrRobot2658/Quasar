import { type ReactNode } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { TenantProvider } from "./context/TenantContext";
import { LangProvider } from "./context/LangContext";
import { AuthProvider, useAuth } from "./context/AuthContext";
import { AssistantProvider } from "./context/AssistantContext";
import AssistantShell from "./components/assistant/AssistantShell";
import LoginPage from "./pages/LoginPage";
import Dashboard from "./pages/Dashboard";
import KnowledgeBasePage from "./pages/KnowledgeBasePage";
import AppsPage from "./pages/AppsPage";
import AnalystPage from "./pages/AnalystPage";
import UserDashboardPage from "./pages/analyst/UserDashboardPage";
import AccountDashboardPage from "./pages/analyst/AccountDashboardPage";
import RoiDashboardPage from "./pages/analyst/RoiDashboardPage";
import CustomDashboardPage from "./pages/analyst/CustomDashboardPage";
import FilterPage from "./pages/FilterPage";
import EtlPage from "./pages/EtlPage";
import EtlFlowPage from "./pages/EtlFlowPage";
import ObjectListPage from "./pages/ObjectListPage";
import ConnectionsPage from "./pages/ConnectionsPage";
import SourceCatalogPage from "./pages/SourceCatalogPage";
import DestinationsPage from "./pages/DestinationsPage";
import UnifyPage from "./pages/UnifyPage";
import ObjectsHubPage from "./pages/ObjectsHubPage";
import AccountsPage from "./pages/AccountsPage";
import AccountDetailPage from "./pages/AccountDetailPage";
import EngagePage from "./pages/EngagePage";
import TagsPage from "./pages/TagsPage";
// 功能模块新增页
import TenantsPage from "./pages/platform/TenantsPage";
import TenantDetailPage from "./pages/platform/TenantDetailPage";
import PipelinesPage from "./pages/PipelinesPage";
import PipelineDetailPage from "./pages/PipelineDetailPage";
import GroupsPage from "./pages/GroupsPage";
import ObjectModelPage from "./pages/ObjectModelPage";
import ObjectRecordDetailPage from "./pages/ObjectRecordDetailPage";
import AccountMergeLogPage from "./pages/AccountMergeLogPage";
import JourneyDetailPage from "./pages/segment/JourneyDetailPage";
import BroadcastDetailPage from "./pages/segment/BroadcastDetailPage";
import TrackingPlanDetailPage from "./pages/segment/TrackingPlanDetailPage";

// Connections
import FunctionsPage from "./pages/segment/FunctionsPage";
import ReverseEtlPage from "./pages/segment/ReverseEtlPage";
import WarehousesPage from "./pages/segment/WarehousesPage";
import SourceDetailPage from "./pages/segment/SourceDetailPage";
// Unify
import ProfileDetailPage from "./pages/segment/ProfileDetailPage";
import IdentityResolutionPage from "./pages/segment/IdentityResolutionPage";
import SqlTraitsPage from "./pages/segment/SqlTraitsPage";
import PredictionsPage from "./pages/segment/PredictionsPage";
import ProfilesSyncPage from "./pages/segment/ProfilesSyncPage";
// Engage
import JourneysPage from "./pages/segment/JourneysPage";
import BroadcastsPage from "./pages/segment/BroadcastsPage";
import AudienceDetailPage from "./pages/segment/AudienceDetailPage";
// Protocols
import TrackingPlansPage from "./pages/segment/TrackingPlansPage";
import ViolationsPage from "./pages/segment/ViolationsPage";
import TransformationsPage from "./pages/segment/TransformationsPage";
// Privacy（真实页，接 /api/privacy/*）
import PrivacyDataControlsPage from "./pages/PrivacyDataControlsPage";
import PrivacyConsentPage from "./pages/PrivacyConsentPage";
import PrivacyDeletionPage from "./pages/PrivacyDeletionPage";
// Monitor（真实页，接 /api/monitor/*）
import MonitorDeliveryPage from "./pages/MonitorDeliveryPage";
import MonitorAlertsPage from "./pages/MonitorAlertsPage";
import MonitorEventLogsPage from "./pages/MonitorEventLogsPage";
// Settings
import SettingsGeneralPage from "./pages/segment/SettingsGeneralPage";
import AccessPage from "./pages/segment/AccessPage";
import TokensPage from "./pages/segment/TokensPage";
import AuditPage from "./pages/segment/AuditPage";
import SettingsMcpPage from "./pages/segment/SettingsMcpPage";

// 生产挂在 /console 下，dev 用根路径
const BASENAME = import.meta.env.PROD ? "/console" : "/";

// 登录硬门禁：未登录跳 /login；鉴权状态未就绪时显示加载中。
function RequireAuth({ children }: { children: ReactNode }) {
  const { user, ready } = useAuth();
  if (!ready) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gray-50">
        <div className="h-6 w-6 animate-spin rounded-full border-2 border-gray-200 border-t-brand-500" />
      </div>
    );
  }
  if (!user) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export default function App() {
  return (
    <TenantProvider>
    <LangProvider>
    <AuthProvider>
    <AssistantProvider>
      <BrowserRouter basename={BASENAME}>
        <Routes>
          {/* 公共路由：登录页 */}
          <Route path="/login" element={<LoginPage />} />

          {/* 受保护应用：全部走登录门禁 */}
          <Route
            path="/*"
            element={
              <RequireAuth>
                <AssistantShell>
                <Routes>
          {/* Overview */}
          <Route path="/" element={<Dashboard />} />
          <Route path="/knowledge" element={<KnowledgeBasePage />} />
          <Route path="/apps" element={<AppsPage />} />
          <Route path="/analyst" element={<AnalystPage />} />
          <Route path="/analyst/dashboards/user" element={<UserDashboardPage />} />
          <Route path="/analyst/dashboards/account" element={<AccountDashboardPage />} />
          <Route path="/analyst/dashboards/roi" element={<RoiDashboardPage />} />
          <Route path="/analyst/dashboards/custom/:id" element={<CustomDashboardPage />} />

          {/* Connections */}
          <Route path="/connections" element={<ConnectionsPage />} />
          <Route path="/connections/catalog" element={<SourceCatalogPage />} />
          <Route path="/connections/destinations" element={<DestinationsPage />} />
          <Route path="/connections/reverse-etl" element={<ReverseEtlPage />} />
          <Route path="/connections/warehouses" element={<WarehousesPage />} />
          <Route path="/connections/functions" element={<FunctionsPage />} />
          <Route path="/connections/flow" element={<EtlFlowPage />} />
          <Route path="/connections/pipelines" element={<PipelinesPage />} />
          <Route path="/connections/pipelines/:id" element={<PipelineDetailPage />} />
          <Route path="/connections/sources/new" element={<EtlPage />} />
          <Route path="/connections/sources/:id" element={<SourceDetailPage />} />

          {/* Unify */}
          <Route path="/unify" element={<UnifyPage />} />
          <Route path="/unify/identity" element={<IdentityResolutionPage />} />
          <Route path="/unify/traits" element={<TagsPage />} />
          <Route path="/unify/sql-traits" element={<SqlTraitsPage />} />
          <Route path="/unify/predictions" element={<PredictionsPage />} />
          <Route path="/unify/sync" element={<ProfilesSyncPage />} />
          <Route path="/unify/groups" element={<GroupsPage />} />
          <Route path="/unify/profiles/:id" element={<ProfileDetailPage />} />

          {/* 对象管理 / 客户管理（一级菜单） */}
          <Route path="/objects" element={<ObjectsHubPage />} />
          <Route path="/objects/model" element={<ObjectModelPage />} />
          <Route path="/objects/:key" element={<ObjectListPage />} />
          <Route path="/objects/:key/:pk" element={<ObjectRecordDetailPage />} />
          <Route path="/accounts" element={<AccountsPage />} />
          <Route path="/accounts/-/merge-log" element={<AccountMergeLogPage />} />
          <Route path="/accounts/:id" element={<AccountDetailPage />} />

          {/* Engage */}
          <Route path="/engage" element={<EngagePage />} />
          <Route path="/engage/audiences/new" element={<FilterPage />} />
          <Route path="/engage/audiences/:id" element={<AudienceDetailPage />} />
          <Route path="/engage/journeys" element={<JourneysPage />} />
          <Route path="/engage/journeys/:id" element={<JourneyDetailPage />} />
          <Route path="/engage/broadcasts" element={<BroadcastsPage />} />
          <Route path="/engage/broadcasts/:id" element={<BroadcastDetailPage />} />

          {/* Protocols */}
          <Route path="/protocols" element={<TrackingPlansPage />} />
          <Route path="/protocols/tracking-plans/:id" element={<TrackingPlanDetailPage />} />
          <Route path="/protocols/violations" element={<ViolationsPage />} />
          <Route path="/protocols/transformations" element={<TransformationsPage />} />

          {/* Privacy */}
          <Route path="/privacy" element={<PrivacyDataControlsPage />} />
          <Route path="/privacy/consent" element={<PrivacyConsentPage />} />
          <Route path="/privacy/deletion" element={<PrivacyDeletionPage />} />

          {/* Monitor */}
          <Route path="/monitor" element={<MonitorDeliveryPage />} />
          <Route path="/monitor/alerts" element={<MonitorAlertsPage />} />
          <Route path="/monitor/logs" element={<MonitorEventLogsPage />} />

          {/* Settings */}
          <Route path="/settings" element={<SettingsGeneralPage />} />
          <Route path="/settings/access" element={<AccessPage />} />
          <Route path="/settings/tokens" element={<TokensPage />} />
          <Route path="/settings/audit" element={<AuditPage />} />
          <Route path="/settings/mcp" element={<SettingsMcpPage />} />
          <Route path="/settings/tenants" element={<TenantsPage />} />
          <Route path="/settings/tenants/:id" element={<TenantDetailPage />} />

          {/* 旧路由别名（外链兼容） */}
          <Route path="/filter" element={<Navigate to="/engage/audiences/new" replace />} />
          <Route path="/etl" element={<Navigate to="/connections/sources/new" replace />} />
          <Route path="/engage/traits" element={<Navigate to="/unify/traits" replace />} />
          <Route path="/unify/objects" element={<Navigate to="/objects" replace />} />
          <Route path="/unify/objects/:key" element={<ObjectListPage />} />
          <Route path="/unify/accounts" element={<Navigate to="/accounts" replace />} />
          <Route path="/unify/accounts/:id" element={<AccountDetailPage />} />
          <Route path="/objects/:key" element={<ObjectListPage />} />

          <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
                </AssistantShell>
              </RequireAuth>
            }
          />
        </Routes>
      </BrowserRouter>
    </AssistantProvider>
    </AuthProvider>
    </LangProvider>
    </TenantProvider>
  );
}
