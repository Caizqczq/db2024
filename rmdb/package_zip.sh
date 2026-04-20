#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="$(basename "$PROJECT_ROOT")"
DEFAULT_OUTPUT="${PROJECT_NAME}.zip"
OUTPUT_NAME="${1:-$DEFAULT_OUTPUT}"
OUTPUT_PATH="$PROJECT_ROOT/$OUTPUT_NAME"

if ! command -v zip >/dev/null 2>&1; then
  echo "错误: 未找到 zip 命令，请先安装 zip。" >&2
  exit 1
fi

PACK_ITEMS=("src" "deps" "CMakeLists.txt")

for item in "${PACK_ITEMS[@]}"; do
  if [[ ! -e "$PROJECT_ROOT/$item" ]]; then
    echo "错误: 未找到打包项（$PROJECT_ROOT/$item）。" >&2
    exit 1
  fi
done

cd "$PROJECT_ROOT"

if [[ -f "$OUTPUT_PATH" ]]; then
  rm -f "$OUTPUT_PATH"
fi

zip -r "$OUTPUT_PATH" "${PACK_ITEMS[@]}" >/dev/null
echo "打包完成: $OUTPUT_PATH"
echo "解压后将直接看到顶层目录: src, deps，以及文件: CMakeLists.txt"
