#!/usr/bin/env python3
"""
Proto Edge Publisher
기존 collector.py가 CSV에 쓰는 대신 MQTT로 발행하는 구조를 시뮬레이션.
실제 Modbus 대신 현재 readings 테이블에서 읽어서 발행.
"""
from __future__ import annotations

import json
import time
import psycopg2
import paho.mqtt.client as mqtt
from datetime import datetime
from zoneinfo import ZoneInfo

BROKER = "localhost"
PORT   = 1883
TOPIC  = "ems/{site}/{unit_id}"

DB = dict(host="127.0.0.1", port=5432, dbname="ems", user="ems",
          password="@c20txkJMiHOXtyoBAslGZ0q", connect_timeout=10)

KST = ZoneInfo("Asia/Seoul")

def get_latest_readings(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (site, unit_id)
                site, unit_id, current_temp, set_temp, cooler, fan, defrost, ts
            FROM readings
            ORDER BY site, unit_id, ts DESC
        """)
        rows = cur.fetchall()
    return [
        dict(site=r[0], unit_id=r[1], current_temp=float(r[2]) if r[2] else None,
             set_temp=float(r[3]) if r[3] else None,
             cooler=r[4], fan=r[5], defrost=r[6],
             ts=r[7].isoformat())
        for r in rows
    ]

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    client.loop_start()

    conn = psycopg2.connect(**DB)
    print(f"[publisher] 시작 — broker={BROKER}:{PORT}")

    try:
        while True:
            readings = get_latest_readings(conn)
            for r in readings:
                topic = TOPIC.format(site=r["site"], unit_id=r["unit_id"])
                payload = json.dumps({
                    "ts":           r["ts"],
                    "current_temp": r["current_temp"],
                    "set_temp":     r["set_temp"],
                    "cooler":       r["cooler"],
                    "fan":          r["fan"],
                    "defrost":      r["defrost"],
                })
                client.publish(topic, payload, qos=1)

            print(f"[publisher] {len(readings)}개 발행 — {datetime.now(KST).strftime('%H:%M:%S')}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("[publisher] 종료")
    finally:
        conn.close()
        client.loop_stop()

if __name__ == "__main__":
    main()
