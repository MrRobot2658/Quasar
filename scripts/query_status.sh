#!/bin/bash
# 查询 ID-Mapping 合并状态

TENANT_ID=${1:-1001}
BASE="http://localhost:8001"

get_channels() {
  case "$1" in
    1001) echo "wechat_openid:oXxx_kafka_user_001 wechat_unionid:union_kafka_abc001 phone:13900002222 wework_extid:wmKafkaExt001 email:kafka_user@brand-a.com device:device_kafka_ios_001" ;;
    1002) echo "wechat_openid:oBbb_kafka_user_001 phone:13800003333 email:kafka_user@brand-b.com device:device_kafka_android_002" ;;
    *)    echo "" ;;
  esac
}

echo "========== 健康检查 =========="
curl -s "$BASE/health" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/health"
echo ""

echo "========== 合并日志 (tenant=$TENANT_ID) =========="
curl -s "$BASE/merge-log/$TENANT_ID" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/merge-log/$TENANT_ID"
echo ""

echo "========== 各渠道 OneID 映射 (tenant=$TENANT_ID) =========="
for pair in $(get_channels "$TENANT_ID"); do
  ctype="${pair%%:*}"
  cid="${pair#*:}"
  result=$(curl -s "$BASE/mapping/$TENANT_ID/$ctype/$cid")
  echo "  $ctype / $cid → $result"
done

echo ""
echo "========== MySQL 直接查询 (tenant=$TENANT_ID) =========="
docker exec agenticdmp-mysql mysql -uagenticdmp -pagenticdmp123 agenticdmp -e "
  SELECT tenant_id, channel_type, channel_id, one_id, source FROM id_mapping WHERE tenant_id=$TENANT_ID ORDER BY one_id;
  SELECT tenant_id, user_id, tags, properties FROM user_profile WHERE tenant_id=$TENANT_ID;
" 2>/dev/null
