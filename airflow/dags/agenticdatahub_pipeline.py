"""AgenticDataHub 通用 pipeline DAG（参数化 + 动态任务展开）。

sql-engine 在「运行 Pipeline」时通过 Airflow REST API 触发本 DAG，把 pipeline 的节点列表
放进 dag_run.conf（nodes=[{id,label,type,kind},...]）。本 DAG 用**动态任务映射**（.expand）
在运行时按 conf 里的节点数**展开成多任务**：plan → run_node × N → finish，每个画布节点对应
一个 task 实例。无需为每个 pipeline 单独建 DAG，也无需在解析期知道节点数。

注：Airflow 在解析期还拿不到 dag_run.conf，故任务**数量**只能在运行时动态展开（mapped
tasks 并行）；画布的连线顺序（edges）暂不体现为任务依赖 —— 如需真实拓扑，可改为按 pipeline
生成独立 DAG 文件。
"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="agenticdatahub_pipeline",
    description="AgenticDataHub 可视化编排 Pipelines 的通用执行 DAG（按 conf 动态展开为多任务）",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["agenticdatahub", "pipeline"],
)
def agenticdatahub_pipeline():
    @task
    def plan(**context) -> list:
        """读取 conf.nodes，归一化为待执行节点列表；为空时兜底单节点。"""
        conf = (context["dag_run"].conf or {})
        nodes = conf.get("nodes") or []
        norm = [{
            "id": str(n.get("id") or f"node-{i}"),
            "label": n.get("label") or n.get("type") or n.get("id") or f"node-{i}",
            "kind": n.get("kind"),
        } for i, n in enumerate(nodes)]
        if not norm:
            norm = [{"id": "task-1", "label": conf.get("pipeline_name", "pipeline"), "kind": None}]
        print(f"[AgenticDataHub] planned {len(norm)} task(s) for pipeline="
              f"{conf.get('pipeline_name')} tenant={conf.get('tenant_id')}")
        return norm

    @task
    def run_node(node: dict, **context) -> str:
        """每个画布节点一个 task 实例（动态展开）。"""
        conf = (context["dag_run"].conf or {})
        print(f"[AgenticDataHub] pipeline={conf.get('pipeline_name')} "
              f"tenant={conf.get('tenant_id')}")
        print(f"  >> run node id={node.get('id')} "
              f"label={node.get('label')} kind={node.get('kind')}")
        return str(node.get("id"))

    @task
    def finish(results: list, **context) -> None:
        conf = (context["dag_run"].conf or {})
        print(f"[AgenticDataHub] pipeline '{conf.get('pipeline_name')}' done · "
              f"ran {len(results)} node(s): {results}")

    finish(run_node.expand(node=plan()))


agenticdatahub_pipeline()
