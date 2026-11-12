#!/usr/bin/env bash
# SFTP 容器首次启动时播种示例批次。
# /seed 是只读挂载的宿主目录；本脚本在容器内以 root 运行，
# 拷贝后把属主改为 SFTP 用户（uid 1001），保证 lab 可读写（归档移动需要写权限）。
set -euo pipefail

HOME_DIR="/home/lab"
MARKER="${HOME_DIR}/.seeded-v1"

if [ -f "${MARKER}" ]; then
  echo "[seed] 已播种过，跳过"
  exit 0
fi

mkdir -p "${HOME_DIR}/inbound" "${HOME_DIR}/archive"

# 已就绪批次（含 READY）：采集启动后即可被采走入档
cp -a /seed/inbound/. "${HOME_DIR}/inbound/"
# 未就绪批次（放在 _waiting，演示 READY 门控：不会被扫描）
if [ -d /seed/_waiting ]; then
  cp -a /seed/_waiting/. "${HOME_DIR}/inbound/"
fi

chown -R 1001:1001 "${HOME_DIR}"
touch "${MARKER}"
chown 1001:1001 "${MARKER}"
echo "[seed] 示例批次播种完成"
