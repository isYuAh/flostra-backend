from __future__ import annotations

import argparse
from pathlib import Path

from definitions_exporter import export_definitions


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="export_definitions",
        description="导出 Flostra backend 的 node/plugin definitions（不启动服务）。",
    )
    p.add_argument(
        "--out-dir",
        default="exported",
        help="输出目录（默认: ./exported）",
    )
    return p


def main() -> int:
    args = _build_parser().parse_args()
    out_dir = Path(args.out_dir)
    paths = export_definitions(out_dir)
    for k, v in paths.items():
        print(f"{k} -> {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

