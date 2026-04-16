# Proto — MQTT + TimescaleDB 파이프라인

기존 CSV→sync→PostgreSQL 구조를 MQTT + TimescaleDB로 전환하는 프로토타입입니다.

## 아키텍처

```
Modbus RTU
    │
edge/collector.py
    ├── CSV 저장 (data/raw/)         ← 기존과 동일, 로컬 백업
    └── MQTT publish → Mosquitto
                          │
               ingestor/ingestor.py
                          │
                  TimescaleDB (proto_readings)
                  - 7일 청크 자동 파티셔닝
                  - 압축/보관 정책 자동 처리
                  - time_bucket()으로 실시간 집계
```

## 기존 대비 제거 가능한 것

| 기존 | 대체 |
|---|---|
| `bin/sync.py` | MQTT가 실시간 전달 |
| `bin/aggregator.py` | `time_bucket()` 쿼리 |
| `temp_1min`, `temp_hourly` 테이블 | `proto_readings` hypertable 하나 |
| `cleanup_old_data()` 함수 | TimescaleDB 보관 정책 자동 처리 |

## 실행

```bash
# 1. DB 스키마 적용
psql -U ems -d ems -f proto/db/schema.sql

# 2. ingestor 실행 (중앙 서버)
python3 proto/ingestor/ingestor.py

# 3. collector 실행 (각 사이트 에지)
python3 proto/edge/collector.py
```

## 집계 쿼리 예시

```sql
-- 1분 집계 (aggregator.py 대체)
SELECT
    time_bucket('1 minute', ts AT TIME ZONE 'Asia/Seoul') AS bucket,
    site, unit_id,
    round(avg(current_temp)::numeric, 2) AS avg_temp,
    round(min(current_temp)::numeric, 2) AS min_temp,
    round(max(current_temp)::numeric, 2) AS max_temp
FROM proto_readings
WHERE ts > NOW() - INTERVAL '1 hour'
GROUP BY bucket, site, unit_id
ORDER BY bucket DESC, unit_id;
```
