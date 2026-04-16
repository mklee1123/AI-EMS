# AI EMS — 에너지 관리 시스템

Modbus RTU 온도 조절기에서 데이터를 수집해 PostgreSQL에 저장하는 다중 사이트 모니터링 파이프라인입니다.  
코드는 공통으로 관리하고, 사이트별 설정만 분리해 운영합니다.

---

## 사이트 현황

| 폴더 | 사이트 | 장비 수 |
|---|---|---|
| [`sites/hr_ngr`](sites/hr_ngr) | 하나로마트 남구로역점 | 7대 (unit 11–17) |
| [`sites/hr_nyg`](sites/hr_nyg) | 하나로마트 노량진점 | 현장 확인 필요 |

---

## 구조

```
AI-EMS/
├── bin/
│   ├── collector.py      # Modbus RTU 수집 (5초 주기)
│   ├── sync.py           # CSV → PostgreSQL 동기화 (3초 주기)
│   ├── aggregator.py     # DB 집계 워커 (60초 주기)
│   └── control.py        # Modbus 제어 모듈
├── db/
│   ├── schema.sql         # DB 스키마
│   └── migrations/        # Alembic 마이그레이션
├── systemd/               # systemd 서비스 파일
├── sites/
│   └── hr_ngr/            # 하나로마트 남구로역점 설정
│       └── site.env.example
├── config.py
├── logging_config.py
└── requirements.txt
```

---

## 데이터 흐름

```
온도 조절기 (Modbus RTU)
        │  5초
        ▼
  collector.py  ──▶  data/raw/{addr}/*.csv  (30일 보관)
                              │  3초
                              ▼
                         sync.py  ──▶  PostgreSQL readings  (1년 보관)
                                               │  60초
                                               ▼
                                        aggregator.py
                                        ├── temp_1min     (7일)
                                        ├── temp_hourly   (90일)
                                        ├── daily_summary (2년)
                                        └── device_status_cache
```

---

## DB 테이블

| 테이블 | 설명 | 보관 |
|---|---|---|
| `readings` | 원시 수집 데이터 (5초 단위) | 1년 |
| `temp_1min` | 1분 집계 | 7일 |
| `temp_hourly` | 1시간 집계 | 90일 |
| `daily_summary` | 일별 요약 | 2년 |
| `device_status_cache` | 장비 현재 상태 | 상시 갱신 |
| `devices` | 장비 메타데이터 | — |

---

## 새 사이트 추가

1. `sites/` 아래 폴더 생성 후 `site.env` 작성
2. 코드 배포 (`git pull`)
3. `site.env` 경로 지정 후 서비스 시작

```bash
# 예시: 새 사이트 추가
mkdir sites/hr_xxx
cp sites/hr_ngr/site.env.example sites/hr_xxx/site.env
# site.env 편집 후 서비스 배포
```

---

## 설치

**1. 환경변수 설정**
```bash
cp sites/hr_ngr/site.env.example sites/hr_ngr/site.env
# site.env 편집 (DB 정보, 시리얼 포트 등)
```

**2. 패키지 설치**
```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

**3. DB 마이그레이션**
```bash
venv/bin/python3 -m alembic upgrade head
```

**4. systemd 서비스 등록**
```bash
sudo bash systemd/install.sh
sudo systemctl start hr-postgres hr-collector hr-sync hr-aggregator
```

---

## 서비스

| 서비스 | 역할 |
|---|---|
| `hr-postgres` | PostgreSQL (로컬 인스턴스) |
| `hr-collector` | Modbus 수집 + CSV 저장 |
| `hr-sync` | CSV → DB 동기화 |
| `hr-aggregator` | 집계 및 상태 캐시 갱신 |

```bash
# 상태 확인
systemctl status hr-postgres hr-collector hr-sync hr-aggregator

# 실시간 로그
journalctl -u hr-collector -f
```

---

## 요구사항

- Python 3.12+
- PostgreSQL 16+
- RS-485 USB 어댑터, `dialout` 그룹 권한 필요
