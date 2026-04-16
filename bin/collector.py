#!/usr/bin/env python3
"""
온도 수집 서비스 (Modbus RTU → CSV)

동작:
  - COLLECTION_INTERVAL(기본 5초)마다 장비 11~17번 폴링
  - 장비별 일별 CSV 저장: data/raw/{unit_id}/addr{unit_id}_YYYYMMDD.csv
  - 통합 스냅샷 갱신: data/snapshot/current.csv
  - 명령 큐 처리: data/cmd/*.json → control 모듈에 위임

설계:
  - 시리얼 포트를 단독으로 소유 (다른 프로세스와 공유 없음)
  - 연결 끊김 시 최대 3회 재연결 후 루프 계속
  - SIGTERM/SIGINT: 현재 사이클 완료 후 클린 종료
"""
from __future__ import annotations

import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional, Tuple

from pymodbus.client import ModbusSerialClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    ADDRESSES, COLLECTION_INTERVAL, DELAY,
    RAW_DIR, SNAPSHOT_DIR, TEMP_DIR,
    SERIAL_PORT, BAUD_RATE, PARITY, STOPBITS, TIMEOUT,
    REG_NOW, REG_DEF, REG_SET, REG_DEV, REG_OFF,
    REG_STATE, REG_FANMODE_WORD,
    COMP_BIT, FAN_BIT, DEF_BIT, FANMODE_BIT,
    load_addresses,
)
from logging_config import configure_logging
import control

configure_logging()

import logging
logger = logging.getLogger(__name__)

# ─── 종료 플래그 ───────────────────────────────────────────────────────────────
_shutdown = False

def _handle_shutdown(sig: int, _frame) -> None:
    global _shutdown
    logger.info("signal %d — 현재 사이클 완료 후 종료", sig)
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)

# ─── Modbus 클라이언트 ─────────────────────────────────────────────────────────
_client: Optional[ModbusSerialClient] = None

def _new_client() -> ModbusSerialClient:
    return ModbusSerialClient(
        port=SERIAL_PORT, baudrate=BAUD_RATE, parity=PARITY,
        stopbits=STOPBITS, bytesize=8, timeout=TIMEOUT,
    )

def _reset_client(delay: float = 0.8) -> None:
    global _client
    if _client:
        try:
            _client.close()
        except Exception:
            pass
    _client = None
    time.sleep(delay)

def _wait_port(timeout: float = 5.0) -> bool:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if os.path.exists(SERIAL_PORT):
            return True
        time.sleep(0.2)
    return os.path.exists(SERIAL_PORT)

def _ensure_client() -> ModbusSerialClient:
    global _client
    if not _wait_port():
        raise RuntimeError(f"시리얼 포트 없음: {SERIAL_PORT}")
    if _client is None:
        _client = _new_client()
    if not _client.connect():
        raise RuntimeError("Modbus 연결 실패")
    return _client

def _with_retry(call, *args, **kwargs):
    """최대 3회 재연결 시도."""
    for attempt in range(3):
        try:
            cli = _ensure_client()
            res = call(cli, *args, **kwargs)
            if getattr(res, "isError", lambda: False)():
                raise IOError("modbus error response")
            return res
        except Exception as exc:
            logger.warning("Modbus 오류 (시도 %d/3): %s", attempt + 1, exc)
            _reset_client()
    raise RuntimeError("연속 3회 Modbus 실패")


# ─── 디코딩 ────────────────────────────────────────────────────────────────────
def _c_from_raw(x: int) -> float:
    return (x - 500) / 10.0

def _dev_from_raw(x: int) -> float:
    return x / 10.0

def _off_from_raw(word: int) -> float:
    low = word & 0xFF
    if low & 0x80:
        low -= 0x100
    return low / 10.0


# ─── 레지스터 읽기 ─────────────────────────────────────────────────────────────
def _read_regs(addr: int, start: int, count: int):
    time.sleep(DELAY)
    rr = _with_retry(
        lambda c: c.read_holding_registers(address=start, count=count, device_id=addr)
    )
    return None if rr.isError() else rr.registers

def read_all(addr: int) -> Optional[Tuple[float, ...]]:
    """
    장비 1대의 전체 값을 읽는다.
    반환: (current_temp, defrost_temp, set_temp, tolerance, correction,
           comp, fan, defrost)
    실패 시 None.
    """
    r0 = _read_regs(addr, REG_NOW, 3)   # now, def, set
    if not r0 or len(r0) < 3:
        return None

    r1 = _read_regs(addr, REG_DEV, 1)
    if not r1:
        return None

    r2 = _read_regs(addr, REG_OFF, 1)
    if not r2:
        return None

    r3 = _read_regs(addr, REG_STATE, 1)
    if not r3:
        return None

    off  = _off_from_raw(r2[0])
    now  = _c_from_raw(r0[0]) - off      # 보정 제거한 실제 온도
    deft = _c_from_raw(r0[1])
    sett = _c_from_raw(r0[2])
    dev  = _dev_from_raw(r1[0])

    state = r3[0]
    comp  = int((state >> COMP_BIT) & 1)
    fan   = int((state >> FAN_BIT)  & 1)
    defc  = int((state >> DEF_BIT)  & 1)

    return (now, deft, sett, dev, off, comp, fan, defc)


# ─── CSV 출력 ──────────────────────────────────────────────────────────────────
def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ymd() -> str:
    return time.strftime("%Y%m%d", time.localtime())

def _fmt_line(addr: int, vals: Tuple, ts: Optional[str] = None) -> str:
    if ts is None:
        ts = _ts()
    now, deft, sett, dev, off, comp, fan, defc = vals
    return (
        f"{ts},{addr},{now:.1f},{deft:.1f},{sett:.1f},"
        f"{dev:.1f},{off:.1f},"
        f"{'ON' if comp else 'OFF'},"
        f"{'ON' if fan else 'OFF'},"
        f"{'ON' if defc else 'OFF'}"
    )

def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line if line.endswith("\n") else line + "\n")

def _safe_write(path: Path, content: str) -> None:
    """atomic write: temp → fsync → rename"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=TEMP_DIR)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

def write_csv(addr: int, vals: Tuple) -> None:
    line = _fmt_line(addr, vals)
    daily = RAW_DIR / str(addr) / f"addr{addr}_{_ymd()}.csv"
    _append_line(daily, line)

def write_snapshot(results: dict, addresses: list) -> None:
    ts = _ts()
    lines = []
    for addr in addresses:
        vals = results.get(addr)
        if vals is None:
            lines.append(f"{ts},{addr},ERR")
        else:
            lines.append(_fmt_line(addr, vals, ts))
    _safe_write(SNAPSHOT_DIR / "current.csv", "\n".join(lines) + "\n")


# ─── 메인 루프 ─────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("collector 시작  port=%s  interval=%ds", SERIAL_PORT, COLLECTION_INTERVAL)

    # 시작 시 오래된 명령 파일 정리
    try:
        control.cleanup_old_cmds()
    except Exception as exc:
        logger.warning("명령 파일 정리 오류: %s", exc)

    while not _shutdown:
        t_cycle = time.monotonic()
        addresses = load_addresses()

        # 수집 전 명령 처리
        try:
            control.process_cmd_queue(_ensure_client())
        except Exception as exc:
            logger.error("명령 큐 오류 (pre): %s", exc, exc_info=True)

        # 장비별 수집
        results: dict = {}
        for addr in addresses:
            if _shutdown:
                break
            try:
                vals = read_all(addr)
            except Exception as exc:
                vals = None
                logger.error("[%d] 읽기 실패: %s", addr, exc, exc_info=True)

            results[addr] = vals
            if vals is not None:
                write_csv(addr, vals)
                logger.debug("[%d] %s", addr, _fmt_line(addr, vals))
            else:
                logger.warning("[%d] 읽기 실패", addr)

        # 스냅샷 갱신
        try:
            write_snapshot(results, addresses)
        except Exception as exc:
            logger.error("스냅샷 쓰기 오류: %s", exc, exc_info=True)

        # 수집 후 명령 처리 (빠른 응답)
        try:
            control.process_cmd_queue(_ensure_client())
        except Exception as exc:
            logger.error("명령 큐 오류 (post): %s", exc, exc_info=True)

        elapsed = time.monotonic() - t_cycle
        sleep_sec = max(0.0, COLLECTION_INTERVAL - elapsed)
        if sleep_sec > 0 and not _shutdown:
            time.sleep(sleep_sec)

    _reset_client(delay=0)
    logger.info("collector 종료")


if __name__ == "__main__":
    main()
