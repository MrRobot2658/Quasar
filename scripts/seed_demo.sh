#!/bin/bash
# 给 privacy / monitor 两个模块灌演示数据，让对应前端页有内容展示。
#
# 用法：bash scripts/seed_demo.sh [tenant_id]   # 默认租户 1001
# 说明：
#   - 全部走应用 API（HTTP JSON），中文经 sql-engine 的 utf8mb4 连接正确入库，避免管道双重编码。
#   - 监控 overview 取「近 60 分钟」聚合，故 monitor_metrics 的桶时间锚定到 DB NOW()。
#   - 删除/抑制工单仅建单不执行（execute_deletion 需 confirm=true），避免误删真实数据。
#   - 非严格幂等：重复执行会追加 delivery-logs / 规则 / 工单等；metrics 按 (租户,源,桶) upsert 累加。
set -uo pipefail

# 宿主常有 HTTP 代理，强制本地直连，避免 curl 走代理挂起
export NO_PROXY='*' no_proxy='*'

T="${1:-1001}"
BASE="${SEED_BASE:-http://localhost:8080/api}"   # 经 nginx 网关，剥 /api 转 sql-engine
MYSQL_CONTAINER="${MYSQL_CONTAINER:-agenticdmp-mysql}"
MYSQL_USER="${MYSQL_USER:-agenticdmp}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-agenticdmp123}"
MYSQL_DATABASE="${MYSQL_DATABASE:-agenticdmp}"

# 用数组承载参数，避免 --noproxy * 之类被 shell glob 展开
CURL=(curl -s --max-time 15)
post() { "${CURL[@]}" -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }

# 从 JSON 响应里取第一个命中的键值（id / category_id / rule_id 等）
jget() { python3 -c "import sys,json
try: d=json.load(sys.stdin)
except Exception: print(''); sys.exit()
for k in sys.argv[1:]:
  if isinstance(d,dict) and d.get(k) is not None: print(d[k]); sys.exit()
print('')" "$@"; }

# ── DB NOW 锚点（监控 overview 取近 60 分钟，桶时间须贴近 DB 时钟）──
NOW=$(docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -B "$MYSQL_DATABASE" -e "SELECT NOW()" 2>/dev/null)
[ -z "$NOW" ] && { echo "无法获取 DB NOW()，请确认 MySQL 容器 $MYSQL_CONTAINER 在运行"; exit 1; }
echo "租户=$T  网关=$BASE  DB NOW=$NOW"
# 注意：macOS date 的 -v 调整必须在 -f 之前；同时用同一时区解析+格式化，纯做减法
mkts() { date -j -v-"$1"M -f "%Y-%m-%d %H:%M:%S" "$NOW" +"%Y-%m-%d %H:%M:%S"; }

echo "===== Monitor: metrics (2 sources × 12 桶/5min) ====="
mc=0
for s in kafka-"$T" web-sdk-"$T"; do
  for i in $(seq 0 11); do
    ts=$(mkts $((i*5)))
    tot=$((1200 - i*40 + RANDOM%120)); fail=$(( (RANDOM%5) + (i==3?70:0) )); succ=$((tot-fail))
    p50=$((20+RANDOM%15)); p95=$((180+RANDOM%120)); p99=$((p95+200))
    body=$(printf '{"bucket_ts":"%s","source":"%s","events_total":%d,"success_count":%d,"failed_count":%d,"latency_ms_p50":%d,"latency_ms_p95":%d,"latency_ms_p99":%d}' \
      "$ts" "$s" "$tot" "$succ" "$fail" "$p50" "$p95" "$p99")
    code=$("${CURL[@]}" -o /dev/null -w "%{http_code}" -X POST "$BASE/monitor/metrics?tenant_id=$T" -H 'Content-Type: application/json' -d "$body")
    [ "$code" = "200" ] && mc=$((mc+1)) || echo "  FAIL $code ts=$ts"
  done
done
echo "  metrics upserted: $mc / 24"

echo "===== Monitor: delivery-logs ====="
dl=0
add_log() { post "/monitor/delivery-logs?tenant_id=$T" "$1" >/dev/null && dl=$((dl+1)); }
add_log "{\"source\":\"kafka-$T\",\"event_name\":\"order_paid\",\"destination\":\"Doris 宽表\",\"status\":\"success\",\"http_code\":200,\"latency_ms\":42}"
add_log "{\"source\":\"kafka-$T\",\"event_name\":\"page_view\",\"destination\":\"Facebook Ads\",\"status\":\"success\",\"http_code\":200,\"latency_ms\":88}"
add_log "{\"source\":\"web-sdk-$T\",\"event_name\":\"add_to_cart\",\"destination\":\"Webhook 营销\",\"status\":\"retry\",\"http_code\":429,\"latency_ms\":510,\"error_message\":\"目的地限流，已重试\"}"
add_log "{\"source\":\"web-sdk-$T\",\"event_name\":\"checkout\",\"destination\":\"飞书机器人\",\"status\":\"failed\",\"http_code\":500,\"latency_ms\":1200,\"error_message\":\"目的地 5xx，投递失败\"}"
add_log "{\"source\":\"kafka-$T\",\"event_name\":\"user_signup\",\"destination\":\"Doris 宽表\",\"status\":\"success\",\"http_code\":200,\"latency_ms\":35}"
add_log "{\"source\":\"web-sdk-$T\",\"event_name\":\"product_view\",\"destination\":\"Google Ads\",\"status\":\"skipped\",\"error_message\":\"用户未授权第三方共享\"}"
echo "  delivery-logs created: $dl"

echo "===== Monitor: alert rules + events ====="
R1=$(post "/monitor/alert-rules?tenant_id=$T" '{"name":"投递成功率过低","metric":"success_rate","operator":"lt","threshold":95,"window_minutes":5,"scope":"all_sources","channel":"feishu","severity":"high","enabled":true}' | jget id rule_id)
R2=$(post "/monitor/alert-rules?tenant_id=$T" "{\"name\":\"P95 延迟过高\",\"metric\":\"latency_p95\",\"operator\":\"gt\",\"threshold\":800,\"window_minutes\":10,\"scope\":\"specific_source\",\"scope_value\":\"web-sdk-$T\",\"channel\":\"webhook\",\"severity\":\"medium\",\"enabled\":true}" | jget id rule_id)
post "/monitor/alert-rules?tenant_id=$T" '{"name":"事件量骤降","metric":"event_count","operator":"lt","threshold":100,"window_minutes":15,"scope":"all_sources","channel":"email","severity":"low","enabled":false}' >/dev/null
echo "  rules: R1=$R1 R2=$R2"
[ -n "$R1" ] && post "/monitor/alert-events?tenant_id=$T" "$(printf '{"rule_id":%s,"metric_value":91.2,"detail":{"source":"all","note":"成功率跌破阈值"}}' "$R1")" >/dev/null && echo "  event for R1 created"
[ -n "$R2" ] && post "/monitor/alert-events?tenant_id=$T" "$(printf '{"rule_id":%s,"metric_value":1120,"detail":{"source":"web-sdk-%s"}}' "$R2" "$T")" >/dev/null && echo "  event for R2 created"

echo "===== Privacy: PII rules ====="
pr=0
add_pii() { post "/privacy/pii/rules" "$1" >/dev/null && pr=$((pr+1)); }
add_pii "{\"tenant_id\":$T,\"field_name\":\"phone\",\"category\":\"联系方式\",\"action\":\"mask\",\"target_objects\":[\"user\",\"lead\"],\"created_by\":\"demo\"}"
add_pii "{\"tenant_id\":$T,\"field_name\":\"email\",\"category\":\"联系方式\",\"action\":\"hash\",\"target_objects\":[\"user\"],\"created_by\":\"demo\"}"
add_pii "{\"tenant_id\":$T,\"field_name\":\"id_card\",\"category\":\"敏感证件\",\"action\":\"encrypt\",\"target_objects\":[\"user\"],\"created_by\":\"demo\"}"
add_pii "{\"tenant_id\":$T,\"field_name\":\"address\",\"category\":\"位置\",\"action\":\"mask\",\"target_objects\":[\"account\"],\"created_by\":\"demo\"}"
echo "  pii rules created: $pr"

echo "===== Privacy: consent categories + records ====="
C1=$(post "/privacy/consent/categories" "{\"tenant_id\":$T,\"category_name\":\"营销推送\",\"description\":\"短信/邮件/Push 营销触达\",\"is_required\":false,\"vendor_list\":[\"飞书\",\"Mailgun\"],\"created_by\":\"demo\"}" | jget id category_id)
C2=$(post "/privacy/consent/categories" "{\"tenant_id\":$T,\"category_name\":\"数据分析\",\"description\":\"行为分析与画像计算\",\"is_required\":true,\"created_by\":\"demo\"}" | jget id category_id)
C3=$(post "/privacy/consent/categories" "{\"tenant_id\":$T,\"category_name\":\"第三方共享\",\"description\":\"广告平台数据共享\",\"is_required\":false,\"vendor_list\":[\"Facebook Ads\",\"Google Ads\"],\"created_by\":\"demo\"}" | jget id category_id)
echo "  categories: C1=$C1 C2=$C2 C3=$C3"
cr=0
add_consent() { post "/privacy/consent" "$1" >/dev/null && cr=$((cr+1)); }
for oid in 100001 100002 100003; do
  [ -n "$C1" ] && add_consent "$(printf '{"tenant_id":%s,"one_id":%s,"category_id":%s,"granted":true}' "$T" "$oid" "$C1")"
  [ -n "$C3" ] && add_consent "$(printf '{"tenant_id":%s,"one_id":%s,"category_id":%s,"granted":false}' "$T" "$oid" "$C3")"
done
echo "  consent records created: $cr"

echo "===== Privacy: deletion / suppression 工单（仅建单不执行）====="
dr=0
add_del() { post "/privacy/deletion" "$1" >/dev/null && dr=$((dr+1)); }
add_del "{\"tenant_id\":$T,\"identifier\":\"13800001111\",\"request_type\":\"delete\",\"reason\":\"用户申请被遗忘权(GDPR)\",\"created_by\":\"demo\"}"
add_del "{\"tenant_id\":$T,\"identifier\":\"user_9527@example.com\",\"request_type\":\"suppress\",\"reason\":\"退订营销\",\"created_by\":\"demo\"}"
add_del "{\"tenant_id\":$T,\"identifier\":\"13700002222\",\"request_type\":\"both\",\"reason\":\"注销账户\",\"created_by\":\"demo\"}"
echo "  deletion requests created: $dr"

echo "===== DONE（打开 /monitor 与 /privacy 查看）====="
