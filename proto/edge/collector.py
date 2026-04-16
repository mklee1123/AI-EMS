#!/usr/bin/env python3
"""
Proto Edge Collector — Modbus RTU → CSV + MQTT

기존 collector.py 대비 변경점:
  - CSV는 기존과 동일하게 항상 저장 (30일 보관, 로컬 백업)
  - MQTT로 추가 발행 (중앙 ingestor → TimescaleDB)
  - MQTT 끊겨도 CSV는 계속 저장되므로 데이터 유실 없음
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusSerialClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "bin"))

from config import (
    ADDRESSES, COLLECTION_INTERVAL, DELAY, SITE,
    RAW_DIR, SNAPSHOT_DIR, TEMP_DIR,
    SERIAL_PORT, BAUD_RATE, PARITY, STOPBITS, TIMEOUT,
    REG_NOW, REG_DEF, REG_SET, REG_DEV, REG_OFF,
    REG_STATE, COMP_BIT, FAN_BIT, DEF_BIT,
    load_addresses,
)
from logging_config import configure_logging
import control

configure_logging()
logger = logging.getLogger(__name__)

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC  = "ems/{site}/{unit_id}"

# ─── 종료 플래그 ───────────────────────────────────────────
_shutdown = False

def _handle_shutdown(sig, _frame):
    global _shutdown
    logger.info("signal %d — 현재 사이클 완료 후 종료", sig)
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)

# ─── MQTT ─────────────────────────────────────────────────
_mqtt_connected = False

def _on_connect(client, userdata, flags, reason_code, properties):
    global _mqtt_connected
    _mqtt_connected = (reason_code == 0)
    if _mqtt_connected:
        logger.info("MQTT 연결 — %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        logger.warning("MQTT 연결 실패 code=%s", reason_code)

def _on_disconnect(client, userdata, flags, reason_code, properties):
    global _mqtt_connected
    _mqtt_connected = False
    logger.warning("MQTT 연결 끊김 — CSV만 저장 중")

def _make_mqtt_client() -> mqtt.Client:
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_connect    = _on_connect
    c.on_disconnect = _on_disconnect
    c.connect_async(MQTT_BROKER, MQTT_PORT)
    c.loop_start()
    return c

# ─── Modbus (기존 collector.py와 동일) ────────────────────
_client: Optional[ModbusSerialClient] = None

def _new_client():
    return ModbusSerialClient(
        port=SERIAL_PORT, baudrate=BAUD_RATE, parity=PARITY,
        stopbits=STOPBITS, bytesize=8, timeout=TIMEOUT,
    )

def _reset_client(delay=0.8):
    global _client
    if _client:
        try: _client.close()
        except Exception: pass
    _client = None
    time.sleep(delay)

def _ensure_client():
    global _client
    if not os.path.exists(SERIAL_PORT):
        raise RuntimeError(f"시리얼 포트 없음: {SERIAL_PORT}")
    if _client is None:
        _client = _new_client()
    if not _client.connect():
        raise RuntimeError("Modbus 연결 실패")
    return _client

def _with_retry(call, *args, **kwargs):
    for attempt in range(3):
        try:
            cli = _ensure_client()
            res = call(cli, *args, **kwargs)
            if getattr(res, "isError", lambda: False)():
                raise IOError("modbus error")
            return res
        except Exception as exc:
            logger.warning("Modbus 오류 (시도 %d/3): %s", attempt + 1, exc)
            _reset_client()
    raise RuntimeError("연속 3회 Modbus 실패")

def _c_from_raw(x):   return (x - 500) / 10.0
def _dev_from_raw(x): return x / 10.0
def _off_from_raw(word):
    low = word & 0xFF
    if low & 0x80: low -= 0x100
    return low / 10.0

def _read_regs(addr, start, count):
    time.sleep(DELAY)
    rr = _with_retry(lambda c: c.read_holding_registers(address=start, count=count, device_id=addr))
    return None if rr.isError() else rr.registers

def read_all(addr: int) -> Optional[Tuple]:
    r0 = _read_regs(addr, REG_NOW, 3)
    if not r0 or len(r0) < 3: return None
    r1 = _read_regs(addr, REG_DEV, 1)
    if not r1: return None
    r2 = _read_regs(addr, REG_OFF, 1)
    if not r2: return None
    r3 = _read_regs(addr, REG_STATE, 1)
    if not r3: return None

    off   = _off_from_raw(r2[0])
    now   = _c_from_raw(r0[0]) - off
    deft  = _c_from_raw(r0[1])
    sett  = _c_from_raw(r0[2])
    dev   = _dev_from_raw(r1[0])
    state = r3[0]
    comp  = bool((state >> COMP_BIT) & 1)
    fan   = bool((state >> FAN_BIT)  & 1)
    defc  = bool((state >> DEF_BIT)  & 1)
    return (now, deft, sett, dev, off, comp, fan, defc)

# ─── CSV 저장 (기존과 동일) ───────────────────────────────
def _ts_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ymd() -> str:
    return time.strftime("%Y%m%d", time.localtime())

def _fmt_line(addr, vals, ts=None) -> str:
    if ts is None: ts = _ts_str()
    now, deft, sett, dev, off, comp, fan, defc = vals
    return (f"{ts},{addr},{now:.1f},{deft:.1f},{sett:.1f},"
            f"{dev:.1f},{off:.1f},"
            f"{'ON' if comp else 'OFF'},"
            f"{'ON' if fan else 'OFF'},"
            f"{'ON' if defc else 'OFF'}")

def write_csv(addr: int, vals: Tuple) -> None:
    line = _fmt_line(addr, vals)
    path = RAW_DIR / str(addr) / f"addr{addr}_{_ymd()}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def write_snapshot(results: dict, addresses: list) -> None:
    ts = _ts_str()
    lines = []
    for addr in addresses:
        vals = results.get(addr)
        lines.append(_fmt_line(addr, vals, ts) if vals else f"{ts},{addr},ERR")
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SNAPSHOT_DIR / ".current.tmp"
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tmp.replace(SNAPSHOT_DIR / "current.csv")

# ─── MQTT publish ─────────────────────────────────────────
def publish(mqtt_client: mqtt.Client, addr: int, vals: Tuple, ts: str) -> bool:
    if not _mqtt_connected:
        return False
    now, deft, sett, dev, off, comp, fan, defc = vals
    topic   = MQTT_TOPIC.format(site=SITE, unit_id=addr)
    payload = json.dumps({
        "ts":           ts,
        "current_temp": round(now,  2),
        "defrost_temp": round(deft, 2),
        "set_temp":     round(sett, 2),
        "tolerance":    round(dev,  2),
        "correction":   round(off,  2),
        "cooler":       comp,
        "fan":          fan,
        "defrost":      defc,
    })
    result = mqtt_client.publish(topic, payload, qos=1)
    return result.rc == mqtt.MQTT_ERR_SUCCESS

# ─── 메인 루프 ────────────────────────────────────────────
def main() -> None:
    logger.info("proto collector 시작  port=%s  broker=%s:%d",
                SERIAL_PORT, MQTT_BROKER, MQTT_PORT)

    mqtt_client = _make_mqtt_client()
    time.sleep(1.0)

    try:
        control.cleanup_old_cmds()
    except Exception as exc:
        logger.warning("명령 파일 정리 오류: %s", exc)

    while not _shutdown:
        t_cycle = time.monotonic()
        addresses = load_addresses()
        ts = datetime.now(timezone.utc).isoformat()

        try:
            control.process_cmd_queue(_ensure_client())
        except Exception as exc:
            logger.error("명령 큐 오류 (pre): %s", exc)

        results: dict = {}
        published = 0
        for addr in addresses:
            if _shutdown:
                break
            try:
                vals = read_all(addr)
            except Exception as exc:
                vals = None
                logger.warning("[%d] 읽기 실패: %s", addr, exc)

            results[addr] = vals
            if vals is not None:
                write_csv(addr, vals)                        # 항상 CSV 저장
                if publish(mqtt_client, addr, vals, ts):     # MQTT 추가 발행
                    published += 1

        logger.info("수집 완료  devices=%d  mqtt=%d  broker=%s",
                    len([v for v in results.values() if v]),
                    published,
                    "connected" if _mqtt_connected else "disconnected")

        try:
            write_snapshot(results, addresses)
        except Exception as exc:
            logger.error("스냅샷 오류: %s", exc)

        try:
            control.process_cmd_queue(_ensure_client())
        except Exception as exc:
            logger.error("명령 큐 오류 (post): %s", exc)

        elapsed   = time.monotonic() - t_cycle
        sleep_sec = max(0.0, COLLECTION_INTERVAL - elapsed)
        if sleep_sec > 0 and not _shutdown:
            time.sleep(sleep_sec)

    _reset_client(delay=0)
    mqtt_client.loop_stop()
    logger.info("proto collector 종료")


if __name__ == "__main__":
    main()
