#!/usr/bin/env python3
"""
데이터 집계 서비스

동작:
  - AGG_INTERVAL(기본 60초)마다 실행
  - readings → temp_1min (최근 5분 재집계)
  - temp_1min → temp_hourly (매 시각 첫 5분 이내)
  - temp_hourly → daily_summary (자정 이후 첫 5분 이내)
  - device_status_cache 갱신 (readings 최신값 기반)
  - 오래된 집계 데이터 정리 (cleanup_old_aggregations())

설계:
  - DB-to-DB SQL 집계 (CSV 파일 미사용)
  - 장비명은 devices 테이블에서 조회
  - SIGTERM/SIGINT: 현재 집계 완료 후 클린 종료
  - 지수 백오프: DB 오류 시 min(2^n, 60)s
"""
from __future__ import annotations

import os
import signal
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo



import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import AGG_INTERVAL, DB_KWARGS, RAW_DIR, SITE
from logging_config import configure_logging

configure_logging()

import logging
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

# ─── 종료 플래그 ───────────────────────────────────────────────────────────────
_shutdown = False

def _handle_shutdown(sig: int, _frame) -> None:
    global _shutdown
    logger.info("signal %d — 현재 집계 완료 후 종료", sig)
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)

# ─── DB 연결 ───────────────────────────────────────────────────────────────────
def _get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_KWARGS, connect_timeout=10)

# ─── 장비명 ────────────────────────────────────────────────────────────────────
def _load_device_names(conn, site: str) -> Dict[int, str]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT unit_id, name FROM devices WHERE site = %s", (site,))
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as exc:
        logger.warning("devices 테이블 조회 실패: %s", exc)
        return {}

# ─── 상태 캐시 갱신 ────────────────────────────────────────────────────────────
def _update_status_cache(conn, site: str, device_names: Dict[int, str]) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (unit_id)
                unit_id, current_temp, set_temp, tolerance,
                correction, cooler, fan, defrost, ts
            FROM readings
            WHERE site = %s AND ts > NOW() - INTERVAL '10 minutes'
            ORDER BY unit_id, ts DESC
        """, (site,))
        rows = cur.fetchall()

    updated = 0
    with conn.cursor() as cur:
        for unit_id, curr, set_t, tol, corr, cooler, fan, defrost, ts in rows:
            tol_f = float(tol) if tol is not None else 1.5
            if curr is not None and set_t is not None:
                dev = abs(float(curr) - float(set_t))
                status = "critical" if dev > tol_f * 2 else "warning" if dev > tol_f else "normal"
            else:
                status = "error"

            cur.execute("""
                INSERT INTO device_status_cache
                  (site, device_id, device_name,
                   current_temp, set_temp, tolerance, correction,
                   compressor, fan, defrost, status, last_reading_time, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (site, device_id) DO UPDATE SET
                    device_name       = EXCLUDED.device_name,
                    current_temp      = EXCLUDED.current_temp,
                    set_temp          = EXCLUDED.set_temp,
                    tolerance         = EXCLUDED.tolerance,
                    correction        = EXCLUDED.correction,
                    compressor        = EXCLUDED.compressor,
                    fan               = EXCLUDED.fan,
                    defrost           = EXCLUDED.defrost,
                    status            = EXCLUDED.status,
                    last_reading_time = EXCLUDED.last_reading_time,
                    updated_at        = NOW()
            """, (
                site, unit_id, device_names.get(unit_id, f"Unit {unit_id}"),
                curr, set_t, tol, corr, cooler, fan, defrost, status, ts,
            ))
            updated += 1

    conn.commit()
    return updated

# ─── 1분 집계 ──────────────────────────────────────────────────────────────────
_SQL_1MIN = """
INSERT INTO temp_1min
  (site, device_id, bucket_time,
   avg_temp, min_temp, max_temp, avg_set_temp,
   compressor_on_ratio, defrost_on_ratio, reading_count)
SELECT
    site,
    unit_id                                                             AS device_id,
    date_trunc('minute', ts AT TIME ZONE 'Asia/Seoul')                 AS bucket_time,
    round(avg(current_temp)::numeric, 2),
    round(min(current_temp)::numeric, 2),
    round(max(current_temp)::numeric, 2),
    round(avg(set_temp)::numeric, 2),
    round(avg(CASE WHEN cooler   THEN 1.0 ELSE 0.0 END)::numeric, 4),
    round(avg(CASE WHEN defrost  THEN 1.0 ELSE 0.0 END)::numeric, 4),
    count(*)
FROM readings
WHERE site = %s AND ts >= %s AND ts < %s AND current_temp IS NOT NULL
GROUP BY site, unit_id, date_trunc('minute', ts AT TIME ZONE 'Asia/Seoul')
ON CONFLICT (site, device_id, bucket_time) DO UPDATE SET
    avg_temp            = EXCLUDED.avg_temp,
    min_temp            = EXCLUDED.min_temp,
    max_temp            = EXCLUDED.max_temp,
    avg_set_temp        = EXCLUDED.avg_set_temp,
    compressor_on_ratio = EXCLUDED.compressor_on_ratio,
    defrost_on_ratio    = EXCLUDED.defrost_on_ratio,
    reading_count       = EXCLUDED.reading_count
"""

def _aggregate_1min(conn, site: str, t_start: datetime, t_end: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(_SQL_1MIN, (site, t_start, t_end))
    conn.commit()

# ─── 시간 집계 ─────────────────────────────────────────────────────────────────
_SQL_HOURLY = """
INSERT INTO temp_hourly
  (site, device_id, bucket_time,
   avg_temp, min_temp, max_temp, avg_set_temp,
   compressor_on_ratio, defrost_on_ratio, reading_count)
SELECT
    site, device_id,
    date_trunc('hour', bucket_time)            AS hour_bucket,
    round(avg(avg_temp)::numeric, 2),
    round(min(min_temp)::numeric, 2),
    round(max(max_temp)::numeric, 2),
    round(avg(avg_set_temp)::numeric, 2),
    round(avg(compressor_on_ratio)::numeric, 4),
    round(avg(defrost_on_ratio)::numeric, 4),
    sum(reading_count)
FROM temp_1min
WHERE site = %s AND bucket_time >= %s AND bucket_time < %s
GROUP BY site, device_id, date_trunc('hour', bucket_time)
ON CONFLICT (site, device_id, bucket_time) DO UPDATE SET
    avg_temp            = EXCLUDED.avg_temp,
    min_temp            = EXCLUDED.min_temp,
    max_temp            = EXCLUDED.max_temp,
    avg_set_temp        = EXCLUDED.avg_set_temp,
    compressor_on_ratio = EXCLUDED.compressor_on_ratio,
    defrost_on_ratio    = EXCLUDED.defrost_on_ratio,
    reading_count       = EXCLUDED.reading_count
"""

def _aggregate_hourly(conn, site: str, hour_start: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(_SQL_HOURLY, (site, hour_start, hour_start + timedelta(hours=1)))
    conn.commit()
    logger.info("시간 집계  %s", hour_start.strftime("%Y-%m-%d %H:00"))

# ─── 일별 집계 ─────────────────────────────────────────────────────────────────
_SQL_DAILY = """
INSERT INTO daily_summary
  (site, device_id, date,
   avg_temp, min_temp, max_temp, avg_set_temp,
   compressor_hours, reading_count)
SELECT
    site, device_id,
    bucket_time::date                           AS day,
    round(avg(avg_temp)::numeric, 2),
    round(min(min_temp)::numeric, 2),
    round(max(max_temp)::numeric, 2),
    round(avg(avg_set_temp)::numeric, 2),
    round(sum(compressor_on_ratio)::numeric, 2),
    sum(reading_count)
FROM temp_hourly
WHERE site = %s AND bucket_time::date = %s
GROUP BY site, device_id, bucket_time::date
ON CONFLICT (site, device_id, date) DO UPDATE SET
    avg_temp         = EXCLUDED.avg_temp,
    min_temp         = EXCLUDED.min_temp,
    max_temp         = EXCLUDED.max_temp,
    avg_set_temp     = EXCLUDED.avg_set_temp,
    compressor_hours = EXCLUDED.compressor_hours,
    reading_count    = EXCLUDED.reading_count
"""

def _aggregate_daily(conn, site: str, target_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(_SQL_DAILY, (site, target_date))
    conn.commit()
    logger.info("일별 집계  %s", target_date)

# ─── 정리 ──────────────────────────────────────────────────────────────────────
def _cleanup_db(conn) -> None:
    """readings(1년) + 집계 테이블 오래된 데이터 정리"""
    with conn.cursor() as cur:
        cur.execute("SELECT cleanup_old_data()")
    conn.commit()
    logger.info("DB 오래된 데이터 정리 완료")

def _cleanup_csv(raw_dir: Path, keep_days: int = 30) -> None:
    """CSV 파일 보관 기간 초과분 삭제"""
    cutoff = time.time() - keep_days * 86400
    deleted = 0
    for csv_file in raw_dir.rglob("*.csv"):
        try:
            if csv_file.stat().st_mtime < cutoff:
                csv_file.unlink()
                deleted += 1
        except Exception as exc:
            logger.warning("CSV 삭제 실패 %s: %s", csv_file.name, exc)
    if deleted:
        logger.info("CSV 정리 완료  삭제=%d개 (보관 기준 %d일)", deleted, keep_days)

# ─── 집계 오케스트레이션 ────────────────────────────────────────────────────────
def run_once(conn, site: str) -> None:
    now = datetime.now(KST)

    # 1. 상태 캐시
    names = _load_device_names(conn, site)
    n = _update_status_cache(conn, site, names)
    logger.info("상태 캐시 갱신  devices=%d", n)

    # 2. 1분 집계: 최근 5개 완료된 분
    minute_now = now.replace(second=0, microsecond=0)
    for i in range(1, 6):
        t_s = minute_now - timedelta(minutes=i)
        _aggregate_1min(conn, site, t_s, t_s + timedelta(minutes=1))
    logger.info("1분 집계 완료  ~%s", (minute_now - timedelta(minutes=1)).strftime("%H:%M"))

    # 3. 시간 집계: 매 시각 첫 5분 이내
    if now.minute < 5:
        _aggregate_hourly(conn, site,
            now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))

    # 4. 일별 집계 + 정리: 자정 이후 첫 5분 이내
    if now.hour == 0 and now.minute < 5:
        _aggregate_daily(conn, site, (now - timedelta(days=1)).date())
        _cleanup_db(conn)
        _cleanup_csv(RAW_DIR, keep_days=30)

# ─── 메인 루프 ─────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("aggregator 시작  site=%s  interval=%ds", SITE, AGG_INTERVAL)
    consecutive_errors = 0

    while not _shutdown:
        t_start = time.monotonic()
        conn: Optional[psycopg2.extensions.connection] = None
        try:
            conn = _get_conn()
            run_once(conn, SITE)
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            backoff = min(2 ** consecutive_errors, 60)
            logger.error("집계 오류  consecutive=%d  backoff=%ds: %s",
                         consecutive_errors, backoff, exc, exc_info=True)
            time.sleep(backoff)
            continue
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        elapsed = time.monotonic() - t_start
        sleep_sec = max(0.0, AGG_INTERVAL - elapsed)
        if sleep_sec > 0 and not _shutdown:
            time.sleep(sleep_sec)

    logger.info("aggregator 종료")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="한 번만 실행")
    args = ap.parse_args()

    if args.once:
        c = _get_conn()
        try:
            run_once(c, SITE)
        finally:
            c.close()
    else:
        main()
