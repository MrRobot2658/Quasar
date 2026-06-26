import type { ChatView } from "../../../api/assistant";
import ProfileCard from "./ProfileCard";
import AudienceCard from "./AudienceCard";
import TableCard from "./TableCard";
import ChartCard from "./ChartCard";
import ChainCard from "./ChainCard";
import FeedbackCard from "./FeedbackCard";

// 渲染指令分发：把 agent 返回的 view 映射到对应内联卡片。
export default function ViewCard({ view }: { view: ChatView }) {
  switch (view.type) {
    case "profile": return <ProfileCard one_id={view.one_id} />;
    case "audience": return <AudienceCard query={view.query} />;
    case "table": return <TableCard object={view.object} query={view.query} />;
    case "chart": return <ChartCard question={view.question} />;
    case "chain": return <ChainCard object={view.object} id={view.id} maxHops={view.max_hops} />;
    case "feedback": return <FeedbackCard topic={view.topic} object_type={view.object_type} />;
    default: return null;
  }
}
