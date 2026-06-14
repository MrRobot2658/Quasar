#!/bin/bash
# 用户画像实时 E2E 测试：微信 / 企微 / 表单 → 身份合并 → 行为汇总 → 宽表
set -euo pipefail

cd "$(dirname "$0")/.."

echo "========== 1. 启动/检查 Docker 服务 =========="
docker compose up -d --build

echo "等待用户画像服务就绪..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8001/health > /dev/null 2>&1; then
    echo "id-mapping 服务已就绪"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "服务启动超时"
    exit 1
  fi
  sleep 2
done

echo ""
echo "========== 2. 执行数据库迁移 =========="
docker exec -i agenticdmp-mysql mysql -uagenticdmp -pagenticdmp123 agenticdmp < sql/migrate_doris.sql
docker exec -i agenticdmp-mysql mysql -uagenticdmp -pagenticdmp123 agenticdmp < sql/migrate_groups.sql
docker exec -i agenticdmp-mysql mysql -uagenticdmp -pagenticdmp123 --default-character-set=utf8mb4 agenticdmp < sql/migrate_tags.sql

echo ""
echo "========== 3. 安装测试依赖 =========="
python3 -m venv .venv 2>/dev/null || true
.venv/bin/pip install -q -r tests/requirements.txt

echo ""
echo "========== 4. 运行用户画像 E2E 测试 =========="
.venv/bin/pytest tests/test_user_profile_realtime.py -v --tb=short

echo ""
echo "========== 测试完成 =========="
