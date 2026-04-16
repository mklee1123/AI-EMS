#!/bin/bash
# systemd 서비스 설치 스크립트
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICES=(hr-postgres hr-collector hr-sync hr-aggregator)

echo "=== HR EMS 서비스 설치 ==="

for svc in "${SERVICES[@]}"; do
    echo "→ $svc.service 설치 중"
    sudo cp "$SCRIPT_DIR/$svc.service" /etc/systemd/system/
done

sudo systemctl daemon-reload

for svc in "${SERVICES[@]}"; do
    sudo systemctl enable "$svc"
    echo "→ $svc enabled"
done

echo ""
echo "완료. 서비스 시작:"
echo "  sudo systemctl start hr-postgres hr-collector hr-sync hr-aggregator"
echo ""
echo "상태 확인:"
echo "  sudo systemctl status hr-postgres hr-collector hr-sync hr-aggregator"
