#!/usr/bin/env bash

set -u

# 总超时时间：30 分钟 (1800 秒)
total_timeout=3600
# 单次尝试超时时间：8 秒
single_pull_timeout=8
start_ts=$(date +%s)

echo "[$(date '+%F %T')] 开始循环执行 git pull，总限时 $((total_timeout / 60)) 分钟。"

while true; do
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  remaining=$((total_timeout - elapsed))

  # 检查总时间是否耗尽
  if [[ $remaining -le 0 ]]; then
    echo "[$(date '+%F %T')] 达到总时间限制，退出。"
    exit 1
  fi

  # 动态调整本次 pull 的超时时间
  pull_timeout=$single_pull_timeout
  if [[ $remaining -lt $pull_timeout ]]; then
    pull_timeout=$remaining
  fi

  echo "[$(date '+%F %T')] 正在执行: git pull (限时 ${pull_timeout}s)..."

  # 执行 git pull
  if timeout -s INT "${pull_timeout}s" git pull; then
    echo "[$(date '+%F %T')] git pull 成功！"
    exit 0
  fi

  code=$?
  if [[ $code -eq 124 ]]; then
    echo "[$(date '+%F %T')] 单次尝试超时 (${pull_timeout}s)，准备重试..."
  else
    echo "[$(date '+%F %T')] git pull 遇到错误 (退出码: $code)，准备重试..."
  fi

  # 稍微等待一下，避免疯狂占用 CPU 或被服务器暂时屏蔽
  sleep 3
done