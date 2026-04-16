"""통합 로깅 설정 — JSON / text 선택 가능"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":      datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
            "module":  record.module,
            "func":    record.funcName,
            "line":    record.lineno,
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: int | None = None) -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    effective  = level if level is not None else getattr(logging, level_name, logging.INFO)
    fmt        = os.getenv("LOG_FORMAT", "text").strip().lower()

    root = logging.getLogger()
    if not root.handlers:
        root.addHandler(logging.StreamHandler())

    root.setLevel(effective)

    if fmt == "json":
        formatter: logging.Formatter = _JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    for handler in root.handlers:
        handler.setFormatter(formatter)

    return root
