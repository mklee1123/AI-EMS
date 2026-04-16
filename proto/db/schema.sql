-- ============================================================
-- Proto: TimescaleDB 스키마
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw 수집 데이터 (hypertable)
CREATE TABLE IF NOT EXISTS proto_readings (
    ts           TIMESTAMPTZ   NOT NULL,
    site         TEXT          NOT NULL,
    unit_id      INTEGER       NOT NULL,
    current_temp NUMERIC(6,2),
    set_temp     NUMERIC(6,2),
    cooler       BOOLEAN,
    fan          BOOLEAN,
    defrost      BOOLEAN
);

-- hypertable: ts 기준 7일 청크
SELECT create_hypertable(
    'proto_readings', 'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 압축 정책: 7일 지난 데이터 자동 압축
ALTER TABLE proto_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'site, unit_id'
);
SELECT add_compression_policy('proto_readings', INTERVAL '7 days', if_not_exists => TRUE);

-- 보관 정책: 1년 후 자동 삭제
SELECT add_retention_policy('proto_readings', INTERVAL '1 year', if_not_exists => TRUE);

-- 장비 메타데이터
CREATE TABLE IF NOT EXISTS proto_devices (
    site    TEXT    NOT NULL,
    unit_id INTEGER NOT NULL,
    name    TEXT    NOT NULL,
    PRIMARY KEY (site, unit_id)
);

INSERT INTO proto_devices VALUES
    ('namguro_station', 11, '냉동식품'),
    ('namguro_station', 12, '육류'),
    ('namguro_station', 13, '주류/건어물'),
    ('namguro_station', 14, '음료'),
    ('namguro_station', 15, '유제품'),
    ('namguro_station', 16, '가공식품'),
    ('namguro_station', 17, '계란')
ON CONFLICT DO NOTHING;
