# -*- coding: utf-8 -*-
"""
data_logger_rest.py
-------------------
Real-time Market Data Logger (Kiwoom REST/WebSocket version)
- 64-bit Python, no OCX, no external parquet exe
- OAuth2 token manager (auto refresh)
- WebSocket real-time (주식체결 + 주식호가잔량 best level)
- Double-buffered parquet writer (pyarrow)
- GUI UX mirrors the original

IMPORTANT: Fill the CONFIG section with your real Kiwoom REST endpoints and
subscription payload format. Then set your APP_KEY / APP_SECRET / ACCOUNT and run.

Author: TickSutra
"""

import os
import sys
import json
import time
import ssl
import math
import queue
import hmac
import base64
import hashlib
import logging
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# 64-bit friendly parquet stack
import pyarrow as pa
import pyarrow.parquet as pq

# HTTP + WS
import requests
from websocket import WebSocketApp  # from websocket-client
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget,
    QPushButton, QLabel, QTextEdit
)
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

# -----------------------------------------------------------------------------
# CONFIG ————> >>>>> FILL THESE WITH YOUR REAL VALUES <<<<<
# -----------------------------------------------------------------------------
APP_KEY = os.environ.get("KIWOOM_APP_KEY", "YOUR_APP_KEY")
APP_SECRET = os.environ.get("KIWOOM_APP_SECRET", "YOUR_APP_SECRET")
ACCOUNT_NO = os.environ.get("KIWOOM_ACCOUNT", "YOUR_ACCOUNT_NO")

# REST base (token & utility calls)
REST_BASE_URL = "https://openapi.kiwoom.com"  # <-- confirm exact host
TOKEN_URL = f"{REST_BASE_URL}/oauth2/tokenP"  # <-- confirm path

# WebSocket base (real-time stream)
WS_URL = "wss://openapi.kiwoom.com/ws"        # <-- confirm path

# Subscription / channel info
# Provide the exact JSON frame that Kiwoom REST WS expects.
# You likely need separate channels for trade(체결) and quote(호가).
# The formatter below produces something like:
# {"header":{"appKey":"...","appSecret":"...","type":"subscribe"},
#  "body":{"channels":[
#    {"name":"stock.trades","symbols":["005930","..."]},
#    {"name":"stock.quotes","symbols":["005930","..."]}
# ]}}
SUBSCRIBE_FMT = {
    "trade_channel": "stock.trades",     # confirm official channel name
    "quote_channel": "stock.quotes",     # confirm official channel name
    "type_sub": "subscribe",
    "type_unsub": "unsubscribe",
}

# Where to write parquet
OUTPUT_DIR = "./data"
BUFFER_SIZE = 100_000
PERIODIC_FLUSH_MS = 15_000  # 15s

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("rest_logger")

# -----------------------------------------------------------------------------
# Token Manager
# -----------------------------------------------------------------------------
class TokenManager:
    """Manages OAuth2 access/refresh tokens for Kiwoom REST."""
    def __init__(self, app_key: str, app_secret: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token: Optional[str] = None
        self.expiry: Optional[datetime] = None

    def _request_token(self) -> None:
        # NOTE: Confirm required body/headers (client credentials vs JWT, etc.)
        # Many KR broker APIs use:
        # {"grant_type":"client_credentials","appkey":..., "appsecret":...}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        r = requests.post(TOKEN_URL, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        # Expected keys may vary; adjust to official spec:
        self.access_token = data.get("access_token") or data.get("accessToken")
        expires_in = int(data.get("expires_in", 1800))
        self.expiry = datetime.utcnow() + timedelta(seconds=expires_in - 30)
        log.info("✓ Access token issued, expires_in=%ss", expires_in)

    def get(self) -> str:
        if not self.access_token or not self.expiry or datetime.utcnow() >= self.expiry:
            self._request_token()
        return self.access_token

# -----------------------------------------------------------------------------
# Parquet Buffer (double-buffered)
# -----------------------------------------------------------------------------
class ParquetBuffer(QObject):
    buffer_flushed = pyqtSignal(int, str)
    error = pyqtSignal(str)

    def __init__(self, buffer_size=BUFFER_SIZE, output_dir=OUTPUT_DIR):
        super().__init__()
        self.buffer_size = buffer_size
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._active: List[Dict[str, Any]] = []
        self._pending: "queue.Queue[List[Dict[str, Any]]]" = queue.Queue()
        self._total = 0

        self._writer = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer.start()

        self._timer = QTimer()
        self._timer.timeout.connect(self._periodic_flush)
        self._timer.start(PERIODIC_FLUSH_MS)

    @property
    def total(self) -> int:
        return self._total

    @property
    def active_len(self) -> int:
        return len(self._active)

    def add(self, rec: Dict[str, Any]) -> None:
        self._active.append(rec)
        self._total += 1
        if len(self._active) >= self.buffer_size:
            self._swap()

    def force_flush(self) -> None:
        if self._active:
            self._swap()

    def stop(self) -> None:
        try:
            self._timer.stop()
        except Exception:
            pass
        self.force_flush()
        self._pending.put(None)
        self._writer.join(timeout=5)

    def _periodic_flush(self) -> None:
        if self._active:
            self._swap()

    def _swap(self) -> None:
        buf = self._active
        self._active = []
        self._pending.put(buf)

    def _writer_loop(self) -> None:
        while True:
            chunk = self._pending.get()
            if chunk is None:
                return
            try:
                self._write_chunk(chunk)
            except Exception as e:
                msg = f"Parquet write error: {e}\n{traceback.format_exc()}"
                log.error(msg)
                self.error.emit(msg)

    def _write_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        if not chunk:
            return
        today = datetime.now().strftime("%Y%m%d")
        path = self.output_dir / f"market_data_{today}.parquet"

        df = pd.DataFrame.from_records(chunk)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if path.exists():
            # append-rowgroup
            with pq.ParquetWriter(path, table.schema, use_dictionary=False) as writer:
                writer.write_table(table)
        else:
            pq.write_table(table, path)

        self.buffer_flushed.emit(len(chunk), str(path))
        log.info("💾 Wrote %s rows to %s", f"{len(chunk):,}", path.name)

# -----------------------------------------------------------------------------
# Symbol Loader (kept same behavior as your OCX version)
# -----------------------------------------------------------------------------
class SymbolLoader:
    @staticmethod
    def load(filepath: str) -> List[str]:
        try:
            if filepath.endswith((".xlsx", ".xls")):
                df = pd.read_excel(filepath)
            else:
                df = pd.read_csv(filepath)

            sym_col = None
            for c in ["Symbol", "symbol", "SYMBOL", "종목코드", "코드"]:
                if c in df.columns:
                    sym_col = c
                    break
            if sym_col is None:
                sym_col = df.columns[0]

            s = df[sym_col].astype(str).str.replace(r"\D", "", regex=True)
            s = s[(s != "") & (s.str.len() <= 6)]
            s = s.str.zfill(6)
            s = s[s.str.len() == 6]
            syms = s.tolist()
            log.info("Loaded %d symbols from %s", len(syms), filepath)
            return syms
        except Exception as e:
            log.error("Symbol load error: %s", e)
            return []

# -----------------------------------------------------------------------------
# WebSocket Real-time Client
# -----------------------------------------------------------------------------
class KiwoomWSClient(QObject):
    connected = pyqtSignal(bool)
    status = pyqtSignal(str)
    data = pyqtSignal(dict)

    def __init__(self, token_mgr: TokenManager):
        super().__init__()
        self.token_mgr = token_mgr
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._symbols: List[str] = []
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> None:
        headers = []
        # Some brokers require the token as a header like:
        # "authorization: Bearer <token>"
        access_token = self.token_mgr.get()
        headers.append(f"authorization: Bearer {access_token}")

        self.ws = WebSocketApp(
            WS_URL,
            header=headers,
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )
        self._thread = threading.Thread(target=self.ws.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}}, daemon=True)
        self._thread.start()
        self.status.emit("Connecting to WebSocket...")

    def disconnect(self) -> None:
        try:
            if self.ws:
                self.ws.close()
            self._connected = False
            self.connected.emit(False)
            self.status.emit("Disconnected")
        except Exception:
            pass

    def subscribe(self, symbols: List[str]) -> None:
        self._symbols = symbols[:]
        if self._connected:
            self._send_subscribe()

    # ---- WS callbacks ----
    def _on_open(self, *_):
        self._connected = True
        self.connected.emit(True)
        self.status.emit("WebSocket connected")
        self._send_subscribe()

    def _on_close(self, *_):
        self._connected = False
        self.connected.emit(False)
        self.status.emit("WebSocket closed")

    def _on_error(self, *_):
        self.status.emit("WebSocket error")
        log.exception("WebSocket error")

    def _on_message(self, _, message: str):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            # Some venues send text frames with key=value; handle if needed
            log.debug("Non-JSON message: %s", message[:200])
            return

        # >>>>> MAP INCOMING FIELDS TO YOUR SCHEMA <<<<<
        # You must match the official message layout for trade vs quote.
        # Below are example extractors—adjust keys per Kiwoom spec.

        evt_type = (data.get("type") or data.get("event") or "").lower()

        # Example structure assumption:
        # {"type":"trade","symbol":"005930","price":"...","change":"...","pct":"...","qty":"...","volcum":"...","venue":"KRX"}
        # {"type":"quote","symbol":"005930","ask1":"...","bid1":"...","ask1_qty":"...","bid1_qty":"...","venue":"KRX"}

        if evt_type in ("trade", "체결"):
            symbol = str(data.get("symbol", "")).zfill(6)
            venue = data.get("venue", "KRX")

            rec = {
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol,
                "venue": venue,
                "real_type": "주식체결",
                "fid_10": str(data.get("price", "")),     # 현재가
                "fid_11": str(data.get("change", "")),    # 전일대비
                "fid_12": str(data.get("pct", "")),       # 등락률
                "fid_27": str(data.get("qty", "")),       # 체결량
                "fid_28": str(data.get("volcum", "")),    # 누적거래량
                "fid_41": "",
                "fid_51": "",
                "fid_61": "",
                "fid_71": "",
                "raw_code": symbol,
            }
            self.data.emit(rec)

        elif evt_type in ("quote", "호가"):
            symbol = str(data.get("symbol", "")).zfill(6)
            venue = data.get("venue", "KRX")

            rec = {
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol,
                "venue": venue,
                "real_type": "주식호가잔량",
                "fid_10": "",
                "fid_11": "",
                "fid_12": "",
                "fid_27": "",
                "fid_28": "",
                "fid_41": str(data.get("ask1", "")),      # 매도호가1
                "fid_51": str(data.get("bid1", "")),      # 매수호가1
                "fid_61": str(data.get("ask1_qty", "")),  # 매도호가수량1
                "fid_71": str(data.get("bid1_qty", "")),  # 매수호가수량1
                "raw_code": symbol,
            }
            self.data.emit(rec)

        else:
            # Heartbeats / system messages can be ignored or logged
            pass

    # ---- helpers ----
    def _send_subscribe(self) -> None:
        if not self.ws or not self._symbols:
            return

        frame = {
            "header": {
                "appKey": APP_KEY,
                "appSecret": APP_SECRET,
                "type": SUBSCRIBE_FMT["type_sub"],
            },
            "body": {
                "channels": [
                    {"name": SUBSCRIBE_FMT["trade_channel"], "symbols": self._symbols},
                    {"name": SUBSCRIBE_FMT["quote_channel"], "symbols": self._symbols},
                ]
            },
        }
        self.ws.send(json.dumps(frame))
        self.status.emit(f"Subscribed {len(self._symbols)} symbols (trade+quote)")

# -----------------------------------------------------------------------------
# GUI (mirrors your original structure/UX)
# -----------------------------------------------------------------------------
class DataLoggerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TickSutra Data Logger (Kiwoom REST)")
        self.setGeometry(120, 120, 860, 600)

        self.token_mgr = TokenManager(APP_KEY, APP_SECRET)
        self.ws_client = KiwoomWSClient(self.token_mgr)
        self.parquet = ParquetBuffer(BUFFER_SIZE, OUTPUT_DIR)
        self.symbol_loader = SymbolLoader()

        self._symbols: List[str] = []
        self._logging_active = False

        # ---- UI ----
        cw = QWidget()
        self.setCentralWidget(cw)
        layout = QVBoxLayout(cw)

        self.title = QLabel("TickSutra Real-time Data Logger (REST/WebSocket)")
        layout.addWidget(self.title)

        btns = QHBoxLayout()
        self.btn_connect = QPushButton("REST 연결")
        self.btn_load = QPushButton("심볼 로드 & 등록")
        self.btn_start = QPushButton("로깅 시작")
        self.btn_stop = QPushButton("로깅 중지")
        self.btn_load.setEnabled(False)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(False)

        btns.addWidget(self.btn_connect)
        btns.addWidget(self.btn_load)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        layout.addLayout(btns)

        self.lbl_conn = QLabel("상태: 연결 안됨")
        self.lbl_syms = QLabel("심볼: 0개 로드됨")
        self.lbl_buf = QLabel(f"버퍼: 0/{BUFFER_SIZE}")
        self.lbl_rec = QLabel("총 레코드: 0")
        self.lbl_token = QLabel("토큰: 확인 전")
        layout.addWidget(self.lbl_conn)
        layout.addWidget(self.lbl_syms)
        layout.addWidget(self.lbl_token)
        layout.addWidget(self.lbl_buf)
        layout.addWidget(self.lbl_rec)

        layout.addWidget(QLabel("로그:"))
        self.log_box = QTextEdit()
        self.log_box.setMaximumHeight(300)
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)

        # ---- signals ----
        self.btn_connect.clicked.connect(self._connect)
        self.btn_load.clicked.connect(self._load_symbols)
        self.btn_start.clicked.connect(self._start_logging)
        self.btn_stop.clicked.connect(self._stop_logging)

        self.ws_client.connected.connect(self._on_ws_connected)
        self.ws_client.status.connect(self._status)
        self.ws_client.data.connect(self._on_data)

        self.parquet.buffer_flushed.connect(self._on_flush)
        self.parquet.error.connect(self._on_error)

        # Status timer
        self._timer = QTimer()
        self._timer.timeout.connect(self._tick)
        self._timer.start(1000)

    # ---- UI helpers ----
    def _log(self, msg: str) -> None:
        t = datetime.now().strftime("%H:%M:%S")
        self.log_box.append(f"[{t}] {msg}")
        sb = self.log_box.verticalScrollBar()
        sb.setValue(sb.maximum())

    # ---- actions ----
    def _connect(self):
        try:
            token = self.token_mgr.get()
            self.lbl_token.setText("토큰: ✅ 발급됨")
            self._log("키움 REST 토큰 발급 성공")
            self.ws_client.connect()
            self.btn_connect.setEnabled(False)
            self.btn_load.setEnabled(True)
        except Exception as e:
            self._log(f"연결 실패: {e}")

    def _load_symbols(self):
        symfile = "./config/symbol_universe.xlsx"
        syms = self.symbol_loader.load(symfile)
        if not syms:
            for alt in ["./symbol_universe.xlsx", "./symbols.xlsx", "./config/symbols.xlsx"]:
                if Path(alt).exists():
                    syms = self.symbol_loader.load(alt)
                    if syms:
                        break
        if not syms:
            self._log("❌ 심볼 파일을 찾을 수 없습니다.")
            return
        self._symbols = syms
        self.lbl_syms.setText(f"심볼: {len(syms)}개 로드됨")
        if self.ws_client.is_connected():
            self.ws_client.subscribe(self._symbols)
            self._log(f"✅ {len(syms)}개 심볼 구독 요청 완료")
            self.btn_start.setEnabled(True)

    def _start_logging(self):
        if not self.ws_client.is_connected():
            self._log("WS 연결 먼저 필요")
            return
        if not self._symbols:
            self._log("심볼 먼저 로드 필요")
            return
        self._logging_active = True
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_load.setEnabled(False)
        self._log("✅ 로깅 시작")

    def _stop_logging(self):
        self._logging_active = False
        self.ws_client.disconnect()
        self.parquet.stop()
        self.btn_stop.setEnabled(False)
        self.btn_start.setEnabled(True)
        self._log("⏹️ 로깅 중지 및 버퍼 플러시 완료")

    # ---- callbacks ----
    def _on_ws_connected(self, ok: bool):
        self.lbl_conn.setText("상태: ✅ 연결됨" if ok else "상태: ❌ 연결 실패")
        if ok and self._symbols:
            self.ws_client.subscribe(self._symbols)
            self.btn_start.setEnabled(True)

    def _status(self, msg: str):
        self._log(f"상태: {msg}")

    def _on_data(self, rec: Dict[str, Any]):
        if self._logging_active:
            self.parquet.add(rec)

    def _on_flush(self, n: int, path: str):
        self._log(f"💾 파일 저장: {Path(path).name} (+{n:,})")

    def _on_error(self, msg: str):
        self._log(f"❌ Parquet 오류: {msg}")

    def _tick(self):
        self.lbl_buf.setText(f"버퍼: {self.parquet.active_len}/{BUFFER_SIZE}")
        self.lbl_rec.setText(f"총 레코드: {self.parquet.total:,}")

    def closeEvent(self, event):
        try:
            self._stop_logging()
        except Exception:
            pass
        event.accept()

# -----------------------------------------------------------------------------
def main():
    # Defensive checks so we don't accidentally run with dummy keys
    if "YOUR_APP_KEY" in APP_KEY or "YOUR_APP_SECRET" in APP_SECRET:
        print("Please set KIWOOM_APP_KEY and KIWOOM_APP_SECRET environment variables.")
        sys.exit(1)

    app = QApplication(sys.argv)
    w = DataLoggerGUI()
    w.show()
    log.info("TickSutra Data Logger started (Kiwoom REST/WebSocket)")
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
