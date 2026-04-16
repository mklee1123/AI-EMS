#!/usr/bin/env python3
"""
CSV → PostgreSQL 동기화 서비스

동작:
  - SYNC_INTERVAL(기본 3초)마다 CSV를 읽어 readings 테이블에 INSERT
  - 장비별 마지막 동기화 시각을 .state/sync_state.json 에 저장
  - ON CONFLICT DO NOTHING 으로 중복 행 안전 처리
  - 배치 최대 5,000건 (SYNC_MAX_BATCH_ROWS 환경변수로 조정)

설계:
  - 영속 커넥션: 매 사이클 열고 닫지 않음. 오류 시 자동 재연결
  - 지수 백오프: 연속 실패 n회 → sleep min(2^n, 60)s
  - SIGTERM/SIGINT: 현재 배치 완료 후 클린 종료
  - .state/sync_health.json 갱신 (watchdog 신선도 확인용)
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import DB_KWARGS, RAW_DIR, SITE, STATE_DIR, SYNC_INTERVAL, load_addresses
from logging_config import configure_logging

configure_logging()

import logging
logger = logging.getLogger(__name__)

# ─── 상수 ──────────────────────────────────────────────────────────────────────
KST            = ZoneInfo("Asia/Seoul")
STATE_FILE     = STATE_DIR / "sync_state.json"
HEALTH_FILE    = STATE_DIR / "sync_health.json"
MAX_BATCH_ROWS = int(os.getenv("SYNC_MAX_BATCH_ROWS", "5000"))

# ─── 종료 플래그 ───────────────────────────────────────────────────────────────
_shutdown = False

def _handle_shutdown(sig: int, _frame) -> None:
    global _shutdown
    logger.info("signal %d — 현재 배치 완료 후 종료", sig)
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)

# ─── 커넥션 관리 ───────────────────────────────────────────────────────────────
_conn: Optional[psycopg2.extensions.connection] = None

def _get_conn() -> psycopg2.extensions.connection:
    global _conn
    if _conn is not None and not _conn.closed:
        return _conn
    _close_conn()
    _conn = psycopg2.connect(**DB_KWARGS, connect_timeout=10)
    logger.debug("DB 연결됨")
    return _conn

def _close_conn() -> None:
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None

# ─── 상태 / 헬스 파일 ──────────────────────────────────────────────────────────
def _load_state() -> Dict[str, str]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}

def _save_state(state: Dict[str, str]) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")
    os.replace(tmp, STATE_FILE)

def _write_health(last_row_ts: str, inserted: int, errors: int) -> None:
    payload = {
        "last_sync_utc":      datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "last_row_ts":        last_row_ts,
        "last_inserted":      inserted,
        "consecutive_errors": errors,
    }
    tmp = HEALTH_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), "utf-8")
    os.replace(tmp, HEALTH_FILE)

# ─── CSV 파싱 ──────────────────────────────────────────────────────────────────
def _kst_parse(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)

def _safe_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def _safe_bool(s: str) -> bool:
    return s.strip().upper() in ("ON", "1", "TRUE", "YES")

def _parse_line(line: str) -> Optional[dict]:
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 10:
        return None
    try:
        ts = _kst_parse(parts[0])
    except ValueError:
        return None
    return {
        "ts":      ts,
        "addr":    parts[1],
        "now":     _safe_float(parts[2]),
        "def":     _safe_float(parts[3]),
        "set":     _safe_float(parts[4]),
        "dev":     _safe_float(parts[5]),
        "off":     _safe_float(parts[6]),
        "comp":    _safe_bool(parts[7]),
        "fan":     _safe_bool(parts[8]),
        "defrost": _safe_bool(parts[9]),
    }

def _extract_ymd(path: Path) -> Optional[str]:
    """addr11_20260128.csv → '20260128'"""
    try:
        token = path.stem.split("_")[1]
        if len(token) == 8 and token.isdigit():
            return token
    except Exception:
        pass
    return None

def _candidate_files(addr: int, last_ts: Optional[datetime]) -> List[Path]:
    addr_dir = RAW_DIR / str(addr)
    if not addr_dir.exists():
        return []
    files = sorted(addr_dir.glob(f"addr{addr}_*.csv"))
    if last_ts is None:
        return files
    cutoff = last_ts.strftime("%Y%m%d")
    return [p for p in files if (y := _extract_ymd(p)) is not None and y >= cutoff]

def _collect_pending(
    addr: int,
    last_ts: Optional[datetime],
    max_rows: int,
) -> Tuple[List[tuple], Optional[datetime]]:
    rows: List[tuple] = []
    max_ts = last_ts

    for csv_path in _candidate_files(addr, last_ts):
        try:
            with csv_path.open("r", encoding="utf-8") as f:
                for raw in f:
                    rec = _parse_line(raw)
                    if rec is None:
                        continue
                    if last_ts and rec["ts"] <= last_ts:
                        continue

                    rows.append((
                        rec["ts"], SITE, rec["addr"],
                        rec["now"], rec["def"], rec["set"],
                        rec["dev"], rec["off"],
                        rec["comp"], rec["fan"], rec["defrost"],
                        None,   # outdoor_temp (별도 수집 시 확장)
                    ))
                    if max_ts is None or rec["ts"] > max_ts:
                        max_ts = rec["ts"]

                    if len(rows) >= max_rows:
                        return rows, max_ts
        except OSError as exc:
            logger.warning("CSV 읽기 오류 %s: %s", csv_path.name, exc)

    return rows, max_ts

# ─── DB INSERT ─────────────────────────────────────────────────────────────────
_INSERT_SQL = """
INSERT INTO readings
  (ts, site, unit_id,
   current_temp, defrost_temp, set_temp, tolerance, correction,
   cooler, fan, defrost, outdoor_temp)
VALUES %s
ON CONFLICT (ts, site, unit_id) DO NOTHING
"""

def _insert_rows(rows: List[tuple]) -> int:
    if not rows:
        return 0
    for attempt in range(2):
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _INSERT_SQL, rows, page_size=1000)
                inserted = max(cur.rowcount, 0)
            conn.commit()
            return inserted
        except psycopg2.OperationalError as exc:
            logger.warning("DB 커넥션 오류 (시도 %d/2): %s", attempt + 1, exc)
            _close_conn()
            time.sleep(0.5 * (2 ** attempt))
        except Exception as exc:
            try:
                _get_conn().rollback()
            except Exception:
                pass
            raise RuntimeError(f"INSERT 실패: {exc}") from exc
    raise RuntimeError("INSERT 2회 연속 실패")

# ─── 메인 루프 ─────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("db-sync 시작  site=%s  interval=%ds  max_batch=%d",
                SITE, SYNC_INTERVAL, MAX_BATCH_ROWS)

    state = _load_state()
    consecutive_errors = 0

    while not _shutdown:
        t_start = time.monotonic()

        try:
            rows: List[tuple] = []
            next_state: Dict[str, str] = {}

            for addr in load_addresses():
                if _shutdown:
                    break
                remaining = MAX_BATCH_ROWS - len(rows)
                if remaining <= 0:
                    break

                last_ts: Optional[datetime] = None
                raw_ts = state.get(str(addr))
                if raw_ts:
                    try:
                        last_ts = _kst_parse(raw_ts)
                    except ValueError:
                        pass

                device_rows, max_ts = _collect_pending(addr, last_ts, remaining)
                rows.extend(device_rows)

                if max_ts and (last_ts is None or max_ts > last_ts):
                    next_state[str(addr)] = max_ts.strftime("%Y-%m-%d %H:%M:%S")

            if rows:
                inserted = _insert_rows(rows)
                state.update(next_state)
                _save_state(state)
                last_row_ts = max(next_state.values()) if next_state else ""
                _write_health(last_row_ts, inserted, 0)
                logger.info("배치 완료  total=%d  inserted=%d  elapsed=%.2fs",
                            len(rows), inserted, time.monotonic() - t_start)
            else:
                _write_health("", 0, 0)

            consecutive_errors = 0

        except Exception as exc:
            consecutive_errors += 1
            backoff = min(2 ** consecutive_errors, 60)
            logger.error("동기화 오류  consecutive=%d  backoff=%ds: %s",
                         consecutive_errors, backoff, exc, exc_info=True)
            _write_health("", 0, consecutive_errors)
            time.sleep(backoff)
            continue

        elapsed = time.monotonic() - t_start
        sleep_sec = max(0.0, SYNC_INTERVAL - elapsed)
        if sleep_sec > 0 and not _shutdown:
            time.sleep(sleep_sec)

    _close_conn()
    logger.info("db-sync 종료")


if __name__ == "__main__":
    main()
