"""
중앙 설정 모듈

우선순위: 환경변수 > site.env > 기본값
모든 서비스가 이 모듈을 임포트해 사용한다.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

# site.env 로드 (환경변수가 이미 있으면 덮어쓰지 않음)
_env_path = Path(__file__).parent / "site.env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)

logger = logging.getLogger(__name__)

# ─── 경로 ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
SNAPSHOT_DIR = DATA_DIR / "snapshot"
CMD_DIR      = DATA_DIR / "cmd"
TEMP_DIR     = DATA_DIR / "temp"
STATE_DIR    = PROJECT_ROOT / ".state"

for _d in [RAW_DIR, SNAPSHOT_DIR, CMD_DIR, TEMP_DIR, STATE_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ─── 사이트 ────────────────────────────────────────────────────────────────────
SITE      = os.getenv("SITE",      "namguro_station")
SITE_NAME = os.getenv("SITE_NAME", "하나로마트 남구로역점")

# ─── Modbus / 시리얼 ───────────────────────────────────────────────────────────
SERIAL_PORT = os.getenv("SERIAL_PORT",
    "/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_BG01CP11-if00-port0")
BAUD_RATE = int(os.getenv("BAUD_RATE", "9600"))
PARITY    = os.getenv("PARITY",    "N")
STOPBITS  = int(os.getenv("STOPBITS", "1"))
TIMEOUT   = float(os.getenv("TIMEOUT", "1.0"))
DELAY     = float(os.getenv("DELAY",   "0.15"))

# ─── 장비 주소 ─────────────────────────────────────────────────────────────────
_DEFAULT_ADDRS = "11,12,13,14,15,16,17"

def _parse_addresses(raw: str) -> list[int]:
    result = []
    for tok in raw.split(","):
        tok = tok.strip()
        if tok.isdigit():
            result.append(int(tok))
    return result

def load_addresses() -> list[int]:
    """환경변수 ADDRESSES → 기본값 11~17"""
    raw = os.getenv("ADDRESSES", _DEFAULT_ADDRS).strip()
    return _parse_addresses(raw) if raw else _parse_addresses(_DEFAULT_ADDRS)

ADDRESSES = load_addresses()

# ─── 레지스터 맵 (우리전자 23C 계열) ──────────────────────────────────────────
REG_NOW          = 0x0000   # 현재온도
REG_DEF          = 0x0001   # 제상온도
REG_SET          = 0x0002   # 설정온도
REG_DEV          = 0x0009   # 허용편차
REG_OFF          = 0x0011   # 보정값
REG_STATE        = 23       # 릴레이 상태
REG_FANMODE_WORD = 16       # 팬 운전 방식 워드

COMP_BIT    = 0
FAN_BIT     = 1
DEF_BIT     = 2
FANMODE_BIT = 4

# ─── 제어 제한값 ────────────────────────────────────────────────────────────────
SET_MIN, SET_MAX, SET_STEP = -22.0, 10.0, 0.1
DEV_MIN, DEV_MAX, DEV_STEP =   0.0,  6.0, 0.1
OFF_MIN, OFF_MAX, OFF_STEP =  -6.0,  6.0, 0.1

# ─── 데이터베이스 ──────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "127.0.0.1")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "ems")
DB_USER     = os.getenv("DB_USER",     "ems")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# psycopg2 용 (특수문자 그대로 전달, URL 인코딩 불필요)
DB_KWARGS = dict(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

# SQLAlchemy / Alembic 용 (@ 등 특수문자 URL 인코딩)
DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ─── 수집/동기화/집계 주기 ────────────────────────────────────────────────────
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "5"))   # 초
SYNC_INTERVAL       = int(os.getenv("SYNC_INTERVAL",       "3"))   # 초
AGG_INTERVAL        = int(os.getenv("AGG_INTERVAL",        "60"))  # 초

# ─── 로그 ──────────────────────────────────────────────────────────────────────
LOG_LEVEL  = os.getenv("LOG_LEVEL",  "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "text").lower()
