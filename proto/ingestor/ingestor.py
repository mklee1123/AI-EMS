#!/usr/bin/env python3
"""
Proto Central Ingestor
MQTT 구독 → TimescaleDB (proto_readings) 삽입.
토픽: ems/{site}/{unit_id}
"""
from __future__ import annotations

import json
import logging
import time
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from queue import Queue, Empty

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BROKER     = "localhost"
PORT       = 1883
TOPIC      = "ems/#"          # 모든 사이트/장비 구독
BATCH_SIZE = 50               # 몇 건씩 묶어서 INSERT
FLUSH_SEC  = 2.0              # 최대 대기 시간

DB = dict(host="127.0.0.1", port=5432, dbname="ems", user="ems",
          password="@c20txkJMiHOXtyoBAslGZ0q", connect_timeout=10)

_queue: Queue = Queue()

# ── MQTT 콜백 ──────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe(TOPIC, qos=1)
    log.info("MQTT 연결 — broker=%s:%d  topic=%s", BROKER, PORT, TOPIC)

def on_message(client, userdata, msg):
    try:
        parts = msg.topic.split("/")   # ems / {site} / {unit_id}
        if len(parts) != 3:
            return
        _, site, unit_id = parts
        data = json.loads(msg.payload)
        _queue.put({
            "ts":           data.get("ts") or datetime.now(timezone.utc).isoformat(),
            "site":         site,
            "unit_id":      int(unit_id),
            "current_temp": data.get("current_temp"),
            "set_temp":     data.get("set_temp"),
            "cooler":       data.get("cooler"),
            "fan":          data.get("fan"),
            "defrost":      data.get("defrost"),
        })
    except Exception as e:
        log.warning("메시지 파싱 오류: %s", e)

# ── DB INSERT ─────────────────────────────────────────────
_INSERT = """
INSERT INTO proto_readings
    (ts, site, unit_id, current_temp, set_temp, cooler, fan, defrost)
VALUES %s
ON CONFLICT DO NOTHING
"""

def flush(conn, batch: list[dict]) -> int:
    rows = [(
        r["ts"], r["site"], r["unit_id"],
        r["current_temp"], r["set_temp"],
        r["cooler"], r["fan"], r["defrost"]
    ) for r in batch]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _INSERT, rows)
    conn.commit()
    return len(rows)

# ── 메인 루프 ─────────────────────────────────────────────
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_start()

    conn = psycopg2.connect(**DB)
    log.info("ingestor 시작")

    total = 0
    batch: list[dict] = []
    last_flush = time.monotonic()

    try:
        while True:
            try:
                item = _queue.get(timeout=0.2)
                batch.append(item)
            except Empty:
                pass

            elapsed = time.monotonic() - last_flush
            if len(batch) >= BATCH_SIZE or (batch and elapsed >= FLUSH_SEC):
                try:
                    n = flush(conn, batch)
                    total += n
                    log.info("INSERT %d건 (누적 %d)", n, total)
                except Exception as e:
                    log.error("INSERT 실패: %s", e)
                    conn = psycopg2.connect(**DB)
                batch = []
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        if batch:
            flush(conn, batch)
        log.info("ingestor 종료  총 %d건", total)
    finally:
        conn.close()
        client.loop_stop()

if __name__ == "__main__":
    main()
