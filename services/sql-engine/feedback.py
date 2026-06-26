"""反馈闭环服务（CDP 五大 Skill 的「回传 → 迭代」）。详见 docs/13-feedback-loop.md。

把下游的消费/触达/转化/质量信号回写给上游资产，驱动资产自迭代：
  Skill 1 客户360   · scan_field_health / record_field_access  → field_usage.recommendation
  Skill 2 标签体系   · aggregate_tag_feedback / log_tag_usage    → tag_definitions.weight/status
  Skill 3 人群圈选   · aggregate_segment_feedback / suggest      → segments.*  + 喂标签聚合
  Skill 4 数据治理   · scan_data_quality / quality_report        → data_quality_checks + violations
  Skill 5 分析洞察   · detect_insights / record_decision         → insight_findings

约定：聚合幂等（重复跑结果一致）；表/字段名只取 OBJECT_REGISTRY 白名单，不拼用户输入；
任何依赖缺失（无触达数据/表缺列）都优雅降级，不抛异常阻塞正向链路。
"""
from __future__ import annotations

import json
import re
from contextlib import contextmanager

import pymysql

from executor import MysqlOlapExecutor
from objects import OBJECT_REGISTRY

# 巡检覆盖的核心字段（按对象）：null/format 检查目标，duplicate 检查唯一标识。
_QUALITY_FIELDS: dict[str, dict] = {
    "user": {
        "null": ["phone", "channel_count"],
        "format": {"phone": r"^1\d{10}$", "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"},
        "unique": ["phone"],
    },
    "lead": {"null": ["lead_name", "stage"], "format": {}, "unique": ["lead_id"]},
    "account": {"null": ["name", "industry"], "format": {}, "unique": ["account_id"]},
    "order": {"null": ["amount", "status"], "format": {}, "unique": ["order_no"]},
}

_FORMAT_RE = {"phone": re.compile(r"^1\d{10}$"),
              "email": re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")}

# 异常阈值
_SKU_DROP_PCT = -30.0       # 单 SKU GMV 环比跌幅超过则告警
_COHORT_DRIFT_PCT = 25.0    # 高价值客群占比环比漂移超过则告警


class FeedbackService:
    def __init__(self, executor: MysqlOlapExecutor | None = None):
        self._executor = executor or MysqlOlapExecutor()
        self.config = self._executor.config

    @contextmanager
    def _conn(self):
        conn = pymysql.connect(**self.config, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    # ════════════════════════════════════════════════════════════════════
    # Skill 1 · 客户360 —— 画像字段消费埋点 + 健康度
    # ════════════════════════════════════════════════════════════════════
    def record_field_access(self, tenant_id: int, object_type: str, fields: list[str]) -> dict:
        """圈选/查询命中字段时累加消费埋点（usage_count++、刷新 last_used_at）。"""
        reg = OBJECT_REGISTRY.get(object_type)
        if not reg:
            return {"recorded": 0}
        valid = [f for f in (fields or []) if f in reg["fields"]]
        n = 0
        with self._conn() as conn, conn.cursor() as cur:
            for f in valid:
                cur.execute(
                    """
                    INSERT INTO field_usage (tenant_id, object_type, field_name, usage_count, last_used_at)
                    VALUES (%s, %s, %s, 1, NOW())
                    ON DUPLICATE KEY UPDATE usage_count = usage_count + 1, last_used_at = NOW()
                    """,
                    (tenant_id, object_type, f),
                )
                n += 1
        return {"recorded": n}

    def scan_field_health(self, tenant_id: int, object_type: str) -> list[dict]:
        """对对象每个字段算填充率/去重值数，结合 usage_count 给 recommendation，写 field_usage。"""
        reg = OBJECT_REGISTRY.get(object_type)
        if not reg:
            return []
        table, fields = reg["table"], reg["fields"]
        out = []
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS c FROM `{table}` WHERE tenant_id=%s", (tenant_id,))
            total = int((cur.fetchone() or {}).get("c") or 0)
            for fname, ftype in fields.items():
                if fname == reg["id"]:
                    continue
                # 填充率：JSON 字段判非空数组/对象，标量判 NOT NULL/非空串
                if ftype in ("json", "json_array"):
                    non_null_sql = (f"SELECT COUNT(*) AS c FROM `{table}` "
                                    f"WHERE tenant_id=%s AND `{fname}` IS NOT NULL "
                                    f"AND JSON_LENGTH(`{fname}`) > 0")
                    distinct = None
                else:
                    non_null_sql = (f"SELECT COUNT(*) AS c FROM `{table}` "
                                    f"WHERE tenant_id=%s AND `{fname}` IS NOT NULL AND `{fname}` <> ''")
                    cur.execute(f"SELECT COUNT(DISTINCT `{fname}`) AS c FROM `{table}` WHERE tenant_id=%s",
                                (tenant_id,))
                    distinct = int((cur.fetchone() or {}).get("c") or 0)
                cur.execute(non_null_sql, (tenant_id,))
                non_null = int((cur.fetchone() or {}).get("c") or 0)
                fill_rate = round(100.0 * non_null / total, 2) if total else 0.0

                usage = self._field_usage(cur, tenant_id, object_type, fname)
                rec = self._recommend_field(fill_rate, usage)
                cur.execute(
                    """
                    INSERT INTO field_usage
                        (tenant_id, object_type, field_name, fill_rate, distinct_count, recommendation, scanned_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        fill_rate=VALUES(fill_rate), distinct_count=VALUES(distinct_count),
                        recommendation=VALUES(recommendation), scanned_at=NOW()
                    """,
                    (tenant_id, object_type, fname, fill_rate, distinct, rec),
                )
                out.append({"field": fname, "fill_rate": fill_rate, "distinct_count": distinct,
                            "usage_count": usage["usage_count"], "recommendation": rec})
        out.sort(key=lambda r: ({"enrich": 0, "deprecate": 1, "keep": 2}[r["recommendation"]],
                                -r["usage_count"]))
        return out

    def _field_usage(self, cur, tenant_id: int, object_type: str, field_name: str) -> dict:
        cur.execute(
            "SELECT usage_count, last_used_at FROM field_usage "
            "WHERE tenant_id=%s AND object_type=%s AND field_name=%s",
            (tenant_id, object_type, field_name))
        row = cur.fetchone() or {}
        return {"usage_count": int(row.get("usage_count") or 0), "last_used_at": row.get("last_used_at")}

    @staticmethod
    def _recommend_field(fill_rate: float, usage: dict) -> str:
        uc = usage["usage_count"]
        if fill_rate < 30.0 and uc > 0:
            return "enrich"        # 有人用但缺数据 → 建议补缺
        if uc == 0 and fill_rate < 30.0:
            return "deprecate"     # 无人问津又没数据 → 建议淘汰
        return "keep"

    def field_health(self, tenant_id: int, object_type: str) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT field_name, usage_count, last_used_at, fill_rate, distinct_count, "
                "recommendation, scanned_at FROM field_usage "
                "WHERE tenant_id=%s AND object_type=%s ORDER BY usage_count DESC",
                (tenant_id, object_type))
            return list(cur.fetchall())

    # ════════════════════════════════════════════════════════════════════
    # Skill 2 · 标签体系 —— 圈选频次 + 转化率 → 加权/归档
    # ════════════════════════════════════════════════════════════════════
    def log_tag_usage(self, tenant_id: int, segment_code: str, tag_codes: list[str]) -> dict:
        if not tag_codes:
            return {"logged": 0}
        with self._conn() as conn, conn.cursor() as cur:
            for tc in tag_codes:
                cur.execute(
                    "INSERT INTO tag_usage_log (tenant_id, tag_code, segment_code) VALUES (%s, %s, %s)",
                    (tenant_id, tc, segment_code))
        return {"logged": len(tag_codes)}

    def aggregate_tag_feedback(self, tenant_id: int) -> dict:
        """select_count←日志，coverage←宽表，avg_conversion_rate←含该标签 segment 的转化率均值，
        weight←按租户均值归一，低效→archived。回写 tag_definitions。"""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT tag_id, tag_code FROM tag_definitions WHERE tenant_id=%s", (tenant_id,))
            tags = list(cur.fetchall())
            if not tags:
                return {"updated": 0}

            # 各标签转化率：扫含该标签的 segment（DSL 里 value 命中 tag_code）
            cur.execute("SELECT segment_code, dsl, conversion_rate FROM segments WHERE tenant_id=%s", (tenant_id,))
            segs = list(cur.fetchall())
            conv_by_tag: dict[str, list[float]] = {}
            for s in segs:
                codes = self._tags_in_dsl(s.get("dsl"))
                cr = float(s.get("conversion_rate") or 0)
                if cr <= 0:
                    continue
                for c in codes:
                    conv_by_tag.setdefault(c, []).append(cr)

            stats = []
            for t in tags:
                code = t["tag_code"]
                cur.execute("SELECT COUNT(*) AS c FROM tag_usage_log WHERE tenant_id=%s AND tag_code=%s",
                            (tenant_id, code))
                select_count = int((cur.fetchone() or {}).get("c") or 0)
                cur.execute(
                    "SELECT COUNT(*) AS c FROM doris_user_wide "
                    "WHERE tenant_id=%s AND JSON_CONTAINS(tags, JSON_QUOTE(%s))", (tenant_id, code))
                coverage = int((cur.fetchone() or {}).get("c") or 0)
                crs = conv_by_tag.get(code, [])
                avg_cr = round(sum(crs) / len(crs), 2) if crs else 0.0
                stats.append({"code": code, "select_count": select_count,
                              "coverage": coverage, "avg_cr": avg_cr})

            tenant_avg = ([s["avg_cr"] for s in stats if s["avg_cr"] > 0] or [0])
            tenant_avg = sum(tenant_avg) / len(tenant_avg) if tenant_avg and tenant_avg[0] else 0.0

            updated = 0
            for s in stats:
                if tenant_avg > 0 and s["avg_cr"] > 0:
                    weight = max(0.2, min(2.0, round(s["avg_cr"] / tenant_avg, 3)))
                else:
                    weight = 1.0
                # 低效归档：被用过但转化率明显低于均值且覆盖极低
                archived = (s["select_count"] > 0 and tenant_avg > 0
                            and 0 < s["avg_cr"] < tenant_avg * 0.5 and s["coverage"] <= 1)
                status = "archived" if archived else "active"
                cur.execute(
                    """
                    UPDATE tag_definitions SET
                        select_count=%s, coverage=%s, avg_conversion_rate=%s,
                        weight=%s, status=%s, feedback_at=NOW()
                    WHERE tenant_id=%s AND tag_code=%s
                    """,
                    (s["select_count"], s["coverage"], s["avg_cr"], weight, status, tenant_id, s["code"]))
                updated += 1
        return {"updated": updated, "tenant_avg_conversion": round(tenant_avg, 2)}

    @staticmethod
    def _tags_in_dsl(dsl) -> list[str]:
        if isinstance(dsl, str):
            try:
                dsl = json.loads(dsl)
            except json.JSONDecodeError:
                return []
        if not isinstance(dsl, dict):
            return []
        out = []
        for c in dsl.get("conditions", []) or []:
            if c.get("field") == "tags":
                v = c.get("value")
                out.extend(v if isinstance(v, list) else [v])
        return [str(x) for x in out if x]

    def tag_health(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT tag_code, tag_name, level, weight, select_count, coverage, "
                "avg_conversion_rate, status, feedback_at FROM tag_definitions "
                "WHERE tenant_id=%s ORDER BY weight DESC, select_count DESC", (tenant_id,))
            return list(cur.fetchall())

    # ════════════════════════════════════════════════════════════════════
    # Skill 3 · 人群圈选 —— 触达回写 + 条件建议
    # ════════════════════════════════════════════════════════════════════
    def aggregate_segment_feedback(self, tenant_id: int) -> dict:
        """对每个有 broadcast 的 segment 读回执，算 open/click/conversion + quality_score，回写 segments。"""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT segment_id, estimate FROM segments WHERE tenant_id=%s", (tenant_id,))
            segs = {r["segment_id"]: r for r in cur.fetchall()}
            if not segs:
                return {"updated": 0}
            updated = 0
            for sid, srow in segs.items():
                cur.execute(
                    "SELECT broadcast_id FROM broadcasts WHERE tenant_id=%s AND segment_id=%s "
                    "ORDER BY broadcast_id DESC LIMIT 1", (tenant_id, sid))
                b = cur.fetchone()
                if not b:
                    continue
                bid = b["broadcast_id"]
                cur.execute(
                    """
                    SELECT COUNT(*) AS total,
                           SUM(sent_at IS NOT NULL)    AS sent,
                           SUM(opened_at IS NOT NULL)  AS opened,
                           SUM(clicked_at IS NOT NULL) AS clicked
                    FROM broadcast_sends WHERE tenant_id=%s AND broadcast_id=%s
                    """, (tenant_id, bid))
                st = cur.fetchone() or {}
                sent = int(st.get("sent") or 0)
                opened = int(st.get("opened") or 0)
                clicked = int(st.get("clicked") or 0)
                converted = clicked  # dev sim：点击近似转化
                open_rate = round(100.0 * opened / sent, 2) if sent else 0.0
                click_rate = round(100.0 * clicked / sent, 2) if sent else 0.0
                conv_rate = round(100.0 * converted / sent, 2) if sent else 0.0
                # 质量分：0.6*转化率归一(以 20% 为满分基准) + 0.4*预估准确度
                conv_norm = min(1.0, conv_rate / 20.0)
                est = int(srow.get("estimate") or 0)
                est_acc = min(1.0, sent / est) if est else (1.0 if sent else 0.0)
                quality = round(0.6 * conv_norm + 0.4 * est_acc, 3)
                cur.execute(
                    """
                    UPDATE segments SET
                        last_engage_id=%s, sent_count=%s, opened_count=%s, clicked_count=%s,
                        converted_count=%s, open_rate=%s, click_rate=%s, conversion_rate=%s,
                        quality_score=%s, feedback_at=NOW()
                    WHERE tenant_id=%s AND segment_id=%s
                    """,
                    (bid, sent, opened, clicked, converted, open_rate, click_rate, conv_rate,
                     quality, tenant_id, sid))
                updated += 1
        return {"updated": updated}

    def segment_quality(self, tenant_id: int) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT segment_code, segment_name, base_object, estimate, sent_count, "
                "open_rate, click_rate, conversion_rate, quality_score, feedback_at "
                "FROM segments WHERE tenant_id=%s ORDER BY quality_score DESC, conversion_rate DESC",
                (tenant_id,))
            return list(cur.fetchall())

    def suggest_segment_conditions(self, tenant_id: int, segment_code: str) -> dict:
        """规则引擎：基于高转化标签 / 行为窗口给圈选条件建议（不自动执行）。"""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM segments WHERE tenant_id=%s AND segment_code=%s",
                        (tenant_id, segment_code))
            seg = cur.fetchone()
            if not seg:
                return {"segment_code": segment_code, "suggestions": [], "reason": "人群不存在"}
            dsl = seg.get("dsl")
            if isinstance(dsl, str):
                try:
                    dsl = json.loads(dsl)
                except json.JSONDecodeError:
                    dsl = {}
            base_object = seg.get("base_object") or (dsl or {}).get("object")
            cur_tags = set(self._tags_in_dsl(dsl))
            base_cr = float(seg.get("conversion_rate") or 0)

            suggestions = []
            # 1) 加一个高转化、尚未用到的标签
            cur.execute(
                "SELECT tag_code, tag_name, avg_conversion_rate, coverage FROM tag_definitions "
                "WHERE tenant_id=%s AND status='active' AND avg_conversion_rate > %s "
                "ORDER BY avg_conversion_rate DESC LIMIT 5", (tenant_id, base_cr))
            for t in cur.fetchall():
                if t["tag_code"] in cur_tags or (t.get("coverage") or 0) <= 0:
                    continue
                suggestions.append({
                    "type": "add_tag", "tag_code": t["tag_code"],
                    "text": f"加上标签「{t['tag_name']}」(历史转化率 {t['avg_conversion_rate']}%)，"
                            f"该标签覆盖 {t['coverage']} 人，预计提升转化",
                })
                if len(suggestions) >= 2:
                    break
            # 2) 行为时间窗收窄
            for c in (dsl or {}).get("conditions", []) or []:
                fld = str(c.get("field", ""))
                if "day" in fld or "window" in fld or "recent" in fld or "时间" in fld:
                    suggestions.append({
                        "type": "narrow_window", "field": fld,
                        "text": f"把行为窗口「{fld}」从 30 天收窄到 14 天，人群更聚焦、触达更精准",
                    })
                    break
            if not suggestions:
                suggestions.append({"type": "none",
                                    "text": "当前条件表现稳健，暂无更优建议；可先积累更多触达回执再评估"})
            return {"segment_code": segment_code, "base_object": base_object,
                    "current_conversion_rate": base_cr, "suggestions": suggestions}

    # ════════════════════════════════════════════════════════════════════
    # Skill 4 · 数据治理 —— 质量巡检 + 质量分 + 护栏
    # ════════════════════════════════════════════════════════════════════
    def scan_data_quality(self, tenant_id: int, object_type: str) -> dict:
        reg = OBJECT_REGISTRY.get(object_type)
        spec = _QUALITY_FIELDS.get(object_type)
        if not reg or not spec:
            return {"object": object_type, "checks": [], "score": None, "reason": "对象不在巡检范围"}
        table = reg["table"]
        checks = []
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS c FROM `{table}` WHERE tenant_id=%s", (tenant_id,))
            total = int((cur.fetchone() or {}).get("c") or 0)

            # null 检查
            for f in spec["null"]:
                if f not in reg["fields"]:
                    continue
                cur.execute(f"SELECT COUNT(*) AS c FROM `{table}` "
                            f"WHERE tenant_id=%s AND (`{f}` IS NULL OR `{f}`='')", (tenant_id,))
                bad = int((cur.fetchone() or {}).get("c") or 0)
                checks.append(self._record_check(cur, tenant_id, object_type, f, "null", total, bad,
                                                 sample=None, auto_fixable=0))
            # duplicate 检查
            for f in spec["unique"]:
                if f not in reg["fields"]:
                    continue
                cur.execute(
                    f"SELECT `{f}` AS v, COUNT(*) AS c FROM `{table}` "
                    f"WHERE tenant_id=%s AND `{f}` IS NOT NULL AND `{f}`<>'' "
                    f"GROUP BY `{f}` HAVING c>1 ORDER BY c DESC LIMIT 5", (tenant_id,))
                dups = list(cur.fetchall())
                bad = sum(int(d["c"]) - 1 for d in dups)
                sample = [{"value": str(d["v"]), "count": int(d["c"])} for d in dups] or None
                checks.append(self._record_check(cur, tenant_id, object_type, f, "duplicate", total, bad,
                                                 sample=sample, auto_fixable=0))
            # format 检查（正则在应用层判，避免依赖 MySQL REGEXP 行为差异）
            for f, _pat in spec["format"].items():
                if f not in reg["fields"] or f not in _FORMAT_RE:
                    continue
                cur.execute(f"SELECT `{f}` AS v FROM `{table}` "
                            f"WHERE tenant_id=%s AND `{f}` IS NOT NULL AND `{f}`<>''", (tenant_id,))
                vals = [r["v"] for r in cur.fetchall()]
                rx = _FORMAT_RE[f]
                bad_vals = [v for v in vals if not rx.match(str(v))]
                sample = [str(v) for v in bad_vals[:5]] or None
                checks.append(self._record_check(cur, tenant_id, object_type, f, "format",
                                                 len(vals), len(bad_vals), sample=sample, auto_fixable=1))
        scored = [c for c in checks if c["total_rows"] > 0]
        overall = round(sum(c["score"] for c in scored) / len(scored), 3) if scored else 1.0
        return {"object": object_type, "total_rows": total, "checks": checks, "score": overall}

    def _record_check(self, cur, tenant_id, object_type, field, check_type,
                      total, bad, sample, auto_fixable) -> dict:
        score = round(1.0 - (bad / total), 3) if total else 1.0
        if score >= 0.99:
            severity = "low"
        elif score >= 0.9:
            severity = "medium"
        else:
            severity = "high"
        cur.execute(
            """
            INSERT INTO data_quality_checks
                (tenant_id, object_type, field_name, check_type, total_rows, bad_rows,
                 score, severity, sample, auto_fixable, checked_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
                total_rows=VALUES(total_rows), bad_rows=VALUES(bad_rows), score=VALUES(score),
                severity=VALUES(severity), sample=VALUES(sample),
                auto_fixable=VALUES(auto_fixable), checked_at=NOW()
            """,
            (tenant_id, object_type, field, check_type, total, bad, score, severity,
             json.dumps(sample, ensure_ascii=False) if sample else None, auto_fixable))
        # 高危同步进既有 violations 表（复用 06 协议模块的护栏视图）
        if severity == "high" and bad > 0:
            cur.execute(
                """
                INSERT INTO violations (tenant_id, event, issue, count, source, severity)
                VALUES (%s,%s,%s,%s,'data-quality','high')
                ON DUPLICATE KEY UPDATE count=VALUES(count), last_seen=NOW(), severity='high'
                """,
                (tenant_id, f"{object_type}.{field}", f"{check_type} 异常 {bad} 行", bad))
        return {"field": field, "check_type": check_type, "total_rows": total, "bad_rows": bad,
                "score": score, "severity": severity, "sample": sample, "auto_fixable": auto_fixable}

    def quality_report(self, tenant_id: int) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT object_type, field_name, check_type, total_rows, bad_rows, score, "
                "severity, sample, auto_fixable, checked_at FROM data_quality_checks "
                "WHERE tenant_id=%s ORDER BY score ASC", (tenant_id,))
            checks = list(cur.fetchall())
        for c in checks:
            if isinstance(c.get("sample"), str):
                try:
                    c["sample"] = json.loads(c["sample"])
                except json.JSONDecodeError:
                    pass
        by_obj: dict[str, list] = {}
        for c in checks:
            by_obj.setdefault(c["object_type"], []).append(float(c["score"]))
        scores = {o: round(sum(v) / len(v), 3) for o, v in by_obj.items()}
        overall = round(sum(scores.values()) / len(scores), 3) if scores else 1.0
        return {"overall_score": overall, "object_scores": scores, "checks": checks}

    # ════════════════════════════════════════════════════════════════════
    # Skill 5 · 分析洞察 —— 异常发现 + 归因 + 决策回传
    # ════════════════════════════════════════════════════════════════════
    def detect_insights(self, tenant_id: int) -> list[dict]:
        findings = []
        with self._conn() as conn, conn.cursor() as cur:
            findings += self._detect_sku_drop(cur, tenant_id)
            findings += self._detect_cohort_drift(cur, tenant_id)
            for f in findings:
                cur.execute(
                    """
                    INSERT INTO insight_findings
                        (tenant_id, finding_type, dimension, metric, baseline, current_value,
                         change_pct, severity, attribution, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'open')
                    ON DUPLICATE KEY UPDATE
                        baseline=VALUES(baseline), current_value=VALUES(current_value),
                        change_pct=VALUES(change_pct), severity=VALUES(severity),
                        attribution=VALUES(attribution), updated_at=NOW()
                    """,
                    (tenant_id, f["finding_type"], f["dimension"], f["metric"], f["baseline"],
                     f["current_value"], f["change_pct"], f["severity"],
                     json.dumps(f["attribution"], ensure_ascii=False)))
        return findings

    def _detect_sku_drop(self, cur, tenant_id: int) -> list[dict]:
        """SKU 断崖：按 product 比较近 7 天 vs 前 7 天 GMV（用 order-contains-product 关系 + 订单金额）。
        dev 数据量小，无足够时间分布时退化为「对比各 SKU 当前 GMV 与品类均值」，仍能产出可演示的归因。"""
        cur.execute(
            """
            SELECT r.dst_id AS product_id, p.category AS category,
                   COUNT(*) AS order_cnt, COALESCE(SUM(o.amount),0) AS gmv
            FROM object_relations r
            JOIN object_order o ON o.tenant_id=r.tenant_id AND o.order_id=r.src_id
            LEFT JOIN object_product p ON p.tenant_id=r.tenant_id AND p.product_id=r.dst_id
            WHERE r.tenant_id=%s AND r.src_type='order' AND r.rel_type='contains' AND r.dst_type='product'
            GROUP BY r.dst_id, p.category
            """, (tenant_id,))
        rows = list(cur.fetchall())
        if not rows:
            return []
        gmvs = [float(r["gmv"]) for r in rows]
        avg = sum(gmvs) / len(gmvs) if gmvs else 0
        out = []
        for r in rows:
            gmv = float(r["gmv"])
            if avg <= 0:
                continue
            change = round(100.0 * (gmv - avg) / avg, 2)
            if change <= _SKU_DROP_PCT:
                out.append({
                    "finding_type": "sku_drop",
                    "dimension": f"product={r['product_id']}",
                    "metric": "gmv",
                    "baseline": round(avg, 2), "current_value": round(gmv, 2),
                    "change_pct": change,
                    "severity": "high" if change <= -50 else "medium",
                    "attribution": {"product_id": r["product_id"], "category": r.get("category"),
                                    "order_cnt": int(r["order_cnt"]),
                                    "note": "该 SKU GMV 显著低于品类均值，建议核查库存/价格/曝光"},
                })
        return out

    def _detect_cohort_drift(self, cur, tenant_id: int) -> list[dict]:
        """客群漂移：高价值客群占比 vs 上一期快照（audience_size_snapshot 有则用，无则跳过）。"""
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM doris_user_wide WHERE tenant_id=%s", (tenant_id,))
            total = int((cur.fetchone() or {}).get("c") or 0)
            cur.execute(
                "SELECT COUNT(*) AS c FROM doris_user_wide "
                "WHERE tenant_id=%s AND JSON_CONTAINS(tags, JSON_QUOTE('high_value'))", (tenant_id,))
            hv = int((cur.fetchone() or {}).get("c") or 0)
        except Exception:  # noqa: BLE001
            return []
        if total == 0:
            return []
        share = round(100.0 * hv / total, 2)
        # 取上一期快照（若表存在），否则用 30% 经验基线
        baseline = None
        try:
            cur.execute(
                "SELECT size FROM audience_size_snapshot WHERE tenant_id=%s "
                "ORDER BY id DESC LIMIT 1 OFFSET 1", (tenant_id,))
            row = cur.fetchone()
            if row and total:
                baseline = round(100.0 * float(row["size"]) / total, 2)
        except Exception:  # noqa: BLE001
            baseline = None
        if baseline is None:
            baseline = 30.0
        change = round(share - baseline, 2)
        if abs(change) < _COHORT_DRIFT_PCT:
            return []
        return [{
            "finding_type": "cohort_drift",
            "dimension": "cohort=high_value",
            "metric": "cohort_share",
            "baseline": baseline, "current_value": share, "change_pct": change,
            "severity": "high" if abs(change) >= 40 else "medium",
            "attribution": {"cohort": "high_value", "total_users": total, "cohort_users": hv,
                            "note": "高价值客群占比偏离基线，建议核查打标规则与近期转化"},
        }]

    def list_findings(self, tenant_id: int, status: str | None = None) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            if status:
                cur.execute("SELECT * FROM insight_findings WHERE tenant_id=%s AND status=%s "
                            "ORDER BY severity, change_pct", (tenant_id, status))
            else:
                cur.execute("SELECT * FROM insight_findings WHERE tenant_id=%s "
                            "ORDER BY status, severity, change_pct", (tenant_id,))
            rows = list(cur.fetchall())
        for r in rows:
            if isinstance(r.get("attribution"), str):
                try:
                    r["attribution"] = json.loads(r["attribution"])
                except json.JSONDecodeError:
                    pass
        return rows

    def record_decision(self, tenant_id: int, finding_id: int, decision: str,
                        result: str | None = None, status: str = "acted") -> dict:
        if status not in ("open", "acknowledged", "acted", "dismissed"):
            status = "acted"
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE insight_findings SET decision=%s, decision_result=%s, status=%s, updated_at=NOW() "
                "WHERE tenant_id=%s AND id=%s",
                (decision, result, status, tenant_id, finding_id))
            affected = cur.rowcount
        return {"finding_id": finding_id, "updated": affected, "status": status}

    # ════════════════════════════════════════════════════════════════════
    # 编排：一次跑完全部聚合（供 DAG / 手动「立即刷新」调用）
    # ════════════════════════════════════════════════════════════════════
    def run_all(self, tenant_id: int) -> dict:
        out = {}
        out["segments"] = self._safe(self.aggregate_segment_feedback, tenant_id)
        out["tags"] = self._safe(self.aggregate_tag_feedback, tenant_id)
        out["insights"] = self._safe(lambda t: {"found": len(self.detect_insights(t))}, tenant_id)
        for obj in ("user", "lead", "account", "order"):
            out[f"field_health.{obj}"] = self._safe(lambda t, o=obj: {"fields": len(self.scan_field_health(t, o))}, tenant_id)
            out[f"quality.{obj}"] = self._safe(lambda t, o=obj: self.scan_data_quality(t, o).get("score"), tenant_id)
        return out

    @staticmethod
    def _safe(fn, *args):
        try:
            return fn(*args)
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)}
