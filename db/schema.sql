-- ============================================================
-- HR EMS 데이터베이스 스키마
-- ============================================================

-- ── 장비 메타데이터 ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS devices (
    site    TEXT    NOT NULL,
    unit_id INTEGER NOT NULL,
    name    TEXT    NOT NULL,
    PRIMARY KEY (site, unit_id)
);

-- 남구로역점 장비 초기 데이터
INSERT INTO devices (site, unit_id, name) VALUES
    ('namguro_station', 11, '냉동식품'),
    ('namguro_station', 12, '육류'),
    ('namguro_station', 13, '주류/건어물'),
    ('namguro_station', 14, '음료'),
    ('namguro_station', 15, '유제품'),
    ('namguro_station', 16, '가공식품'),
    ('namguro_station', 17, '계란')
ON CONFLICT DO NOTHING;

-- ── Raw 수집 데이터 ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS readings (
    ts           TIMESTAMPTZ  NOT NULL,
    site         TEXT         NOT NULL,
    unit_id      INTEGER      NOT NULL,
    current_temp NUMERIC(6,2),
    defrost_temp NUMERIC(6,2),
    set_temp     NUMERIC(6,2),
    tolerance    NUMERIC(6,2),
    correction   NUMERIC(6,2),
    cooler       BOOLEAN,
    fan          BOOLEAN,
    defrost      BOOLEAN,
    outdoor_temp NUMERIC(6,2),
    PRIMARY KEY (ts, site, unit_id)
);

CREATE INDEX IF NOT EXISTS idx_readings_site_ts
    ON readings (site, ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_site_unit_ts
    ON readings (site, unit_id, ts DESC);

-- ── 1분 집계 ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS temp_1min (
    id                  SERIAL    PRIMARY KEY,
    site                TEXT      NOT NULL,
    device_id           INTEGER   NOT NULL,
    bucket_time         TIMESTAMP NOT NULL,
    avg_temp            NUMERIC(6,2),
    min_temp            NUMERIC(6,2),
    max_temp            NUMERIC(6,2),
    avg_set_temp        NUMERIC(6,2),
    compressor_on_ratio NUMERIC(5,4),
    defrost_on_ratio    NUMERIC(5,4),
    reading_count       INTEGER   DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (site, device_id, bucket_time)
);

CREATE INDEX IF NOT EXISTS idx_temp_1min_site_device_time
    ON temp_1min (site, device_id, bucket_time DESC);

-- ── 1시간 집계 ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS temp_hourly (
    id                  SERIAL    PRIMARY KEY,
    site                TEXT      NOT NULL,
    device_id           INTEGER   NOT NULL,
    bucket_time         TIMESTAMP NOT NULL,
    avg_temp            NUMERIC(6,2),
    min_temp            NUMERIC(6,2),
    max_temp            NUMERIC(6,2),
    avg_set_temp        NUMERIC(6,2),
    compressor_on_ratio NUMERIC(5,4),
    defrost_on_ratio    NUMERIC(5,4),
    reading_count       INTEGER   DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (site, device_id, bucket_time)
);

CREATE INDEX IF NOT EXISTS idx_temp_hourly_site_device_time
    ON temp_hourly (site, device_id, bucket_time DESC);

-- ── 일별 요약 ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_summary (
    id               SERIAL  PRIMARY KEY,
    site             TEXT    NOT NULL,
    device_id        INTEGER NOT NULL,
    date             DATE    NOT NULL,
    avg_temp         NUMERIC(6,2),
    min_temp         NUMERIC(6,2),
    max_temp         NUMERIC(6,2),
    avg_set_temp     NUMERIC(6,2),
    compressor_hours NUMERIC(5,2),
    reading_count    INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (site, device_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_site_device_date
    ON daily_summary (site, device_id, date DESC);

-- ── 현재 상태 캐시 ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS device_status_cache (
    site              TEXT        NOT NULL,
    device_id         INTEGER     NOT NULL,
    device_name       TEXT,
    current_temp      NUMERIC(6,2),
    set_temp          NUMERIC(6,2),
    tolerance         NUMERIC(6,2),
    correction        NUMERIC(6,2),
    compressor        BOOLEAN     DEFAULT FALSE,
    fan               BOOLEAN     DEFAULT FALSE,
    defrost           BOOLEAN     DEFAULT FALSE,
    status            TEXT        DEFAULT 'normal',
    last_reading_time TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (site, device_id)
);

-- ── 데이터 보관 정리 함수 ─────────────────────────────────────
CREATE OR REPLACE FUNCTION cleanup_old_data() RETURNS void AS $$
BEGIN
    -- raw 수집 데이터: 1년 보관
    DELETE FROM readings    WHERE ts          < NOW() - INTERVAL '1 year';
    -- 1분 집계: 7일 보관
    DELETE FROM temp_1min   WHERE bucket_time < NOW() - INTERVAL '7 days';
    -- 시간 집계: 90일 보관
    DELETE FROM temp_hourly WHERE bucket_time < NOW() - INTERVAL '90 days';
    -- 일별 요약: 2년 보관
    DELETE FROM daily_summary WHERE date      < CURRENT_DATE - INTERVAL '2 years';
END;
$$ LANGUAGE plpgsql;
