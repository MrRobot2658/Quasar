// 连接器目录（单一事实源）：数据源 Sources 与数据仓库 Warehouses 共用。
// 与后端 connections_api 的 source_type / warehouse_type 字符串对齐；新增连接器只加目录、不动连接逻辑。
import {
  FileSpreadsheet, Database, Radio, Cloud, Code, Server, Zap, Leaf, Search,
  Snowflake, Warehouse, Layers, Triangle, HardDrive, type LucideIcon,
} from "lucide-react";

export type ConnectorSurface = "source" | "warehouse";
export type ConnectorCategory = "file" | "api" | "streaming" | "database" | "warehouse" | "lake" | "object";

export interface Connector {
  key: string;
  label: string;
  icon: LucideIcon;
  category: ConnectorCategory;
  surfaces: ConnectorSurface[];
}

export const CONNECTORS: Connector[] = [
  // 文件 / API / 流（既有）
  { key: "csv", label: "CSV / Paste", icon: FileSpreadsheet, category: "file", surfaces: ["source"] },
  { key: "api", label: "REST API", icon: Cloud, category: "api", surfaces: ["source"] },
  { key: "javascript", label: "JavaScript", icon: Code, category: "api", surfaces: ["source"] },
  { key: "kafka", label: "Kafka", icon: Radio, category: "streaming", surfaces: ["source"] },
  // 数据库
  { key: "mysql", label: "MySQL", icon: Database, category: "database", surfaces: ["source", "warehouse"] },
  { key: "postgres", label: "PostgreSQL", icon: Database, category: "database", surfaces: ["source", "warehouse"] },
  { key: "sqlserver", label: "SQL Server", icon: Server, category: "database", surfaces: ["source"] },
  { key: "oracle", label: "Oracle", icon: Database, category: "database", surfaces: ["source"] },
  { key: "clickhouse", label: "ClickHouse", icon: Zap, category: "database", surfaces: ["source", "warehouse"] },
  { key: "mongodb", label: "MongoDB", icon: Leaf, category: "database", surfaces: ["source"] },
  { key: "elasticsearch", label: "Elasticsearch", icon: Search, category: "database", surfaces: ["source"] },
  // 数据仓库 / OLAP
  { key: "doris", label: "Apache Doris", icon: Warehouse, category: "warehouse", surfaces: ["warehouse"] },
  { key: "hive", label: "Apache Hive", icon: Warehouse, category: "warehouse", surfaces: ["warehouse"] },
  { key: "snowflake", label: "Snowflake", icon: Snowflake, category: "warehouse", surfaces: ["source", "warehouse"] },
  { key: "bigquery", label: "Google BigQuery", icon: Cloud, category: "warehouse", surfaces: ["source", "warehouse"] },
  { key: "redshift", label: "Amazon Redshift", icon: Warehouse, category: "warehouse", surfaces: ["source", "warehouse"] },
  // 数据湖 / 湖仓
  { key: "iceberg", label: "Apache Iceberg", icon: Layers, category: "lake", surfaces: ["source", "warehouse"] },
  { key: "delta", label: "Delta Lake", icon: Triangle, category: "lake", surfaces: ["source", "warehouse"] },
  // 对象存储
  { key: "s3", label: "Amazon S3", icon: HardDrive, category: "object", surfaces: ["source"] },
];

export const connectorByKey = (k: string): Connector | undefined => CONNECTORS.find((c) => c.key === k);

const CATEGORY_ORDER: ConnectorCategory[] = ["file", "api", "streaming", "database", "warehouse", "lake", "object"];

export function categoryLabel(c: ConnectorCategory, tr: (zh: string, en?: string) => string): string {
  const m: Record<ConnectorCategory, [string, string]> = {
    file: ["文件 / 粘贴", "File / Paste"],
    api: ["API / 函数", "API / Function"],
    streaming: ["流 / 消息", "Streaming"],
    database: ["数据库", "Databases"],
    warehouse: ["数据仓库", "Warehouses"],
    lake: ["数据湖 / 湖仓", "Data Lakes"],
    object: ["对象存储", "Object Storage"],
  };
  return tr(...m[c]);
}

/** 按类别分组某个使用面（source / warehouse）的连接器，用于下拉 optgroup。 */
export function groupBySurface(surface: ConnectorSurface): { category: ConnectorCategory; items: Connector[] }[] {
  const list = CONNECTORS.filter((c) => c.surfaces.includes(surface));
  return CATEGORY_ORDER
    .map((category) => ({ category, items: list.filter((c) => c.category === category) }))
    .filter((g) => g.items.length > 0);
}
