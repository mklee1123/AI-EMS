"""
Modbus 제어 모듈

레지스터 쓰기 및 명령 큐 처리를 담당한다.
시리얼 포트를 직접 열지 않는다 — collector.py가 클라이언트를 소유하고
필요할 때 이 모듈의 함수를 호출한다.

지원 필드:
  set     — 설정온도
  dev     — 허용편차
  off     — 보정값
  fanmode — 팬 운전 방식 (0: 압축기 연동, 1: FULL)
"""
from __future__ import annotations

import glob
import json
import logging
import os
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pymodbus.client import ModbusSerialClient

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    CMD_DIR, DELAY,
    REG_SET, REG_DEV, REG_OFF, REG_FANMODE_WORD, FANMODE_BIT,
    SET_MIN, SET_MAX, SET_STEP,
    DEV_MIN, DEV_MAX, DEV_STEP,
    OFF_MIN, OFF_MAX, OFF_STEP,
)

logger = logging.getLogger(__name__)


# ─── 인코딩 ─────────────────────────────────────────────────────────────────
def _round_step(x: float, step: float) -> float:
    return round(round(x / step) * step, 10)

def _c_to_raw(c: float) -> int:
    return int(round(c * 10 + 500))

def _dev_to_raw(d: float) -> int:
    return int(round(d * 10))

def _off_to_raw(o: float) -> int:
    return int(round(o * 10)) & 0xFF


# ─── 레지스터 쓰기 ──────────────────────────────────────────────────────────
def _write_reg(client: "ModbusSerialClient", addr: int, reg: int, value: int) -> bool:
    time.sleep(DELAY)
    rq = client.write_register(address=reg, value=value, device_id=addr)
    return not getattr(rq, "isError", lambda: True)()


def _read_reg(client: "ModbusSerialClient", addr: int, reg: int) -> int | None:
    time.sleep(DELAY)
    rr = client.read_holding_registers(address=reg, count=1, device_id=addr)
    if getattr(rr, "isError", lambda: True)():
        return None
    return rr.registers[0]


def do_write(
    client: "ModbusSerialClient",
    addr: int,
    field: str,
    value: float,
) -> bool:
    """
    단일 필드 쓰기. 성공 시 True 반환.
    범위 초과 시 False 반환 (Modbus 통신 시도 없음).
    """
    if field == "set":
        v = _round_step(value, SET_STEP)
        if not (SET_MIN <= v <= SET_MAX):
            logger.warning("[%d] set 범위 초과: %.1f", addr, v)
            return False
        return _write_reg(client, addr, REG_SET, _c_to_raw(v))

    elif field == "dev":
        v = _round_step(value, DEV_STEP)
        if not (DEV_MIN <= v <= DEV_MAX):
            logger.warning("[%d] dev 범위 초과: %.1f", addr, v)
            return False
        return _write_reg(client, addr, REG_DEV, _dev_to_raw(v))

    elif field == "off":
        v = _round_step(value, OFF_STEP)
        if not (OFF_MIN <= v <= OFF_MAX):
            logger.warning("[%d] off 범위 초과: %.1f", addr, v)
            return False
        cur = _read_reg(client, addr, REG_OFF)
        if cur is None:
            return False
        new = (cur & 0xFF00) | _off_to_raw(v)
        return _write_reg(client, addr, REG_OFF, new)

    elif field == "fanmode":
        v = int(value)
        if v not in (0, 1):
            logger.warning("[%d] fanmode 값 오류: %d", addr, v)
            return False
        cur = _read_reg(client, addr, REG_FANMODE_WORD)
        if cur is None:
            return False
        new = (cur | (1 << FANMODE_BIT)) if v == 1 else (cur & ~(1 << FANMODE_BIT))
        return _write_reg(client, addr, REG_FANMODE_WORD, new)

    else:
        logger.warning("[%d] 알 수 없는 필드: %s", addr, field)
        return False


# ─── 명령 큐 처리 ──────────────────────────────────────────────────────────
def process_cmd_queue(client: "ModbusSerialClient") -> None:
    """data/cmd/*.json 파일을 읽어 순서대로 실행한다."""
    files = sorted(CMD_DIR.glob("*.json"))
    for fp in files:
        try:
            with fp.open("r", encoding="utf-8") as f:
                cmd = json.load(f)

            addr  = int(cmd["addr"])
            field = str(cmd["reg"])
            value = float(cmd["value"])

            ok = do_write(client, addr, field, value)
            logger.info("cmd %s → addr=%d reg=%s val=%s : %s",
                        fp.name, addr, field, value, "OK" if ok else "FAIL")

            if ok:
                fp.unlink()
            else:
                fp.rename(fp.with_suffix(".fail"))

        except Exception:
            logger.error("cmd 처리 예외 (%s):\n%s", fp.name, traceback.format_exc())
            try:
                fp.rename(fp.with_suffix(".err"))
            except Exception:
                pass


def cleanup_old_cmds(days: int = 30) -> None:
    """30일 지난 .fail / .err 파일 삭제"""
    cutoff = time.time() - days * 86400
    for ext in ("*.fail", "*.err"):
        for fp in CMD_DIR.glob(ext):
            try:
                if fp.stat().st_mtime < cutoff:
                    fp.unlink()
                    logger.info("오래된 명령 파일 삭제: %s", fp.name)
            except Exception as exc:
                logger.warning("삭제 실패 %s: %s", fp.name, exc)
