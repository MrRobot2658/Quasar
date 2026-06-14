#!/bin/bash
# 在 Docker 容器内运行 Kafka 模拟（走完整 Kafka → ID-Mapping 链路）

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ARGS="${*:- --all}"

echo "========== Kafka 链路模拟 (Docker) =========="
echo "参数: $ARGS"
echo ""

docker run --rm \
  --network agenticdmp_agenticdmp-net \
  -v "$SCRIPT_DIR/simulate_channels.py:/app/simulate_channels.py:ro" \
  agenticdmp-id-mapping \
  python /app/simulate_channels.py $ARGS --bootstrap kafka:29092

echo ""
echo "等待 ID-Mapping 服务消费 Kafka 消息..."
sleep 10

echo ""
echo "========== 查询租户 1001 合并结果 =========="
bash "$SCRIPT_DIR/query_status.sh" 1001

echo ""
echo "========== 查询租户 1002 合并结果 =========="
bash "$SCRIPT_DIR/query_status.sh" 1002
