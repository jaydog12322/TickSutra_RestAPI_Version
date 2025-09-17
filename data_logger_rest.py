# -*- coding: utf-8 -*-
"""
TickSutra — Kiwoom REST/WebSocket Data Logger (64-bit)
- OAuth token (appkey/secretkey) -> token/expires_dt
- WebSocket connect -> LOGIN -> REG (subscribe)
- Double-buffer Parquet writer via pyarrow
- GUI similar to your existing flow

Requirements:
  pip install requests websocket-client pyarrow pandas PyQt5

Set env:
  KIWOOM_APP_KEY, KIWOOM_APP_SECRET
"""

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env

import os, sys, json, ssl, queue, threading, traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from websocket import WebSocketApp

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QTextEdit
)
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-18s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tiksutra.rest")

# -----------------------------------------------------------------------------
# CONFIG —> fill only where noted
# -----------------------------------------------------------------------------
APP_KEY    = os.getenv("KIWOOM_APP_KEY")
APP_SECRET = os.getenv("KIWOOM_APP_SECRET")

# REST token endpoint (from your docs)
REST_BASE_URL = "https://api.kiwoom.com"
TOKEN_URL     = f"{REST_BASE_URL}/oauth2/token"

# WebSocket endpoint (from your screenshot)
WS_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"

# Subscription controls:
# type=["00"] means 주문체결(계좌단위 알림). Add other 실시간 항목 코드(s) as needed.
# For symbols-based feeds, set SUBSCRIBE_ITEMS to a list of 종목코드 strings.
SUBSCRIBE_GROUP   = "1"
SUBSCRIBE_REFRESH = "1"
SUBSCRIBE_TYPES   = ["00"]   # add more types if needed (e.g., 체결/호가 codes)
SUBSCRIBE_ITEMS   = [""]     # "" per Kiwoom sample for 00(주문체결). Put "005930" etc. when required.

OUTPUT_DIR         = "./data"
BUFFER_SIZE        = 100_000
PERIODIC_FLUSH_MS  = 15_000

# -----------------------------------------------------------------------------
# Token Manager (matches your response: token/expires_dt/token_type)
# -----------------------------------------------------------------------------
class TokenManager:
    def __init__(self, app_key: str, app_secret: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token: Optional[str] = None
        self.expiry: Optional[datetime] = None

    def _request_token(self) -> None:
        headers = {"Content-Type": "application/json;charset=UTF-8"}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        r = requests.post(TOKEN_URL, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()

        # Expected: {"token": "...", "expires_dt":"YYYYMMDDHHMMSS", "token_type":"bearer", ...}
        if str(data.get("return_code")) not in ("0", "None", "none"):
            raise RuntimeError(f"Token request failed: {data}")

        tok = data.get("token")
        if not tok:
            raise RuntimeError(f"Token missing in response: {data}")

        self.access_token = tok

        exp_str = data.get("expires_dt")
        if exp_str and len(exp_str) == 14:
            exp_local = datetime.strptime(exp_str, "%Y%m%d%H%M%S")
            # Safety pad; server time may be KST; relative margin is fine.
            self.expiry = exp_local - timedelta(seconds=30)
        else:
            self.expiry = datetime.utcnow() + timedelta(minutes=30)

        logging.info("✓ Token ready; expires_dt=%s", data.get("expires_dt"))

    def get(self) -> str:
        if not self.access_token or not self.expiry or datetime.utcnow() >= self.expiry:
            self._request_token()
        return self.access_token

# -----------------------------------------------------------------------------
# Parquet Buffer (double-buffered writer)
# -----------------------------------------------------------------------------
class ParquetBuffer(QObject):
    buffer_flushed = pyqtSignal(int, str)
    error = pyqtSignal(str)

    def __init__(self, buffer_size=BUFFER_SIZE, output_dir=OUTPUT_DIR):
        super().__init__()
        self.buffer_size = buffer_size
        self.output_dir = Path(output_dir); self.output_dir.mkdir(parents=True, exist_ok=True)
        self._active: List[Dict[str, Any]] = []
        self._pending: "queue.Queue[List[Dict[str, Any]]]" = queue.Queue()
        self._total = 0

        self._writer = threading.Thread(target=self._writer_loop, daemon=True); self._writer.start()
        self._timer = QTimer(); self._timer.timeout.connect(self._periodic_flush); self._timer.start(PERIODIC_FLUSH_MS)

    @property
    def total(self) -> int: return self._total
    @property
    def active_len(self) -> int: return len(self._active)

    def add(self, rec: Dict[str, Any]) -> None:
        self._active.append(rec); self._total += 1
        if len(self._active) >= self.buffer_size: self._swap()

    def force_flush(self) -> None:
        if self._active: self._swap()

    def stop(self) -> None:
        try: self._timer.stop()
        except Exception: pass
        self.force_flush(); self._pending.put(None); self._writer.join(timeout=5)

    def _periodic_flush(self) -> None:
        if self._active: self._swap()

    def _swap(self) -> None:
        buf = self._active; self._active = []; self._pending.put(buf)

    def _writer_loop(self) -> None:
        while True:
            chunk = self._pending.get()
            if chunk is None: return
            try: self._write_chunk(chunk)
            except Exception as e:
                msg = f"Parquet write error: {e}\n{traceback.format_exc()}"; logging.error(msg); self.error.emit(msg)

    def _write_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        if not chunk: return
        today = datetime.now().strftime("%Y%m%d")
        path  = self.output_dir / f"market_data_{today}.parquet"
        df    = pd.DataFrame.from_records(chunk)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if path.exists():
            with pq.ParquetWriter(path, table.schema, use_dictionary=False) as writer:
                writer.write_table(table)
        else:
            pq.write_table(table, path)
        self.buffer_flushed.emit(len(chunk), str(path))
        logging.info("💾 wrote %s rows -> %s", f"{len(chunk):,}", path.name)

# -----------------------------------------------------------------------------
# WebSocket client — matches Kiwoom's LOGIN / REG / PING protocol
# -----------------------------------------------------------------------------
class KiwoomWSClient(QObject):
    connected = pyqtSignal(bool)
    status    = pyqtSignal(str)
    data      = pyqtSignal(dict)

    def __init__(self, token_mgr: TokenManager):
        super().__init__()
        self.token_mgr = token_mgr
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = False

        self._reg_sent = False
        self._items: List[str] = SUBSCRIBE_ITEMS[:]
        self._types: List[str] = SUBSCRIBE_TYPES[:]

    def connect(self) -> None:
        # Note: Kiwoom sample logs in via message (not header)
        self.ws = WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )
        self._thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
            daemon=True
        )
        self._thread.start()
        self.status.emit("Connecting to WS...")

    def set_subscription(self, items: List[str], types: List[str]) -> None:
        self._items = items[:]
        self._types = types[:]

    # ----- WS callbacks -----
    def _on_open(self, *_):
        self._connected = True
        self.connected.emit(True)
        self.status.emit("WS connected")

        # LOGIN frame
        login = {"trnm": "LOGIN", "token": self.token_mgr.get()}
        self.ws.send(json.dumps(login))
        self.status.emit("LOGIN sent")

    def _on_close(self, *_):
        self._connected = False
        self.connected.emit(False)
        self.status.emit("WS closed")

    def _on_error(self, *_):
        self.status.emit("WS error"); logging.exception("WebSocket error")

    def _on_message(self, _, message: str):
        try:
            msg = json.loads(message)
        except Exception:
            logging.debug("Non-JSON WS message: %s", message[:200])
            return

        trnm = str(msg.get("trnm", "")).upper()

        # Handle LOGIN result
        if trnm == "LOGIN":
            if str(msg.get("return_code")) == "0":
                self.status.emit("LOGIN ok")
                # Auto-send REG once after login
                if not self._reg_sent:
                    self._send_reg()
                    self._reg_sent = True
            else:
                self.status.emit(f"LOGIN failed: {msg.get('return_msg')}")
                return

        # Echo PING
        elif trnm == "PING":
            try:
                self.ws.send(json.dumps(msg))
            except Exception: pass
            return

        else:
            # Any other real-time payload —> forward to logger
            # We store both mapped fields (if present) and raw payload.
            rec = self._map_message(msg)
            self.data.emit(rec)

    # ----- helpers -----
    def _send_reg(self):
        frame = {
            "trnm": "REG",
            "grp_no": SUBSCRIBE_GROUP,
            "refresh": SUBSCRIBE_REFRESH,
            "data": [{
                "item":  self._items,   # e.g., ["005930", ...] or [""] for 00
                "type":  self._types,   # e.g., ["00"] now; add more when needed
            }]
        }
        self.ws.send(json.dumps(frame))
        self.status.emit(f"REG sent: items={self._items} types={self._types}")

    def _map_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generic mapper: keep your fid_* schema.
        For now we try common keys if present; always store raw payload too.
        Update these mappings once you confirm each 실시간 항목's field names.
        """
        now = datetime.now().isoformat()
        # Common guess keys (adjust when you have the exact spec)
        symbol = msg.get("symbol") or msg.get("code") or msg.get("sym") or ""
        price  = msg.get("price")  or msg.get("tp")   or msg.get("현재가") or ""
        change = msg.get("change") or msg.get("cp")   or msg.get("전일대비") or ""
        pct    = msg.get("pct")    or msg.get("rp")   or msg.get("등락률") or ""
        qty    = msg.get("qty")    or msg.get("tqty") or msg.get("체결량") or ""
        tvol   = msg.get("tvol")   or msg.get("vol")  or msg.get("누적거래량") or ""
        ask1   = msg.get("ask1")   or msg.get("aq1")  or msg.get("매도호가1") or ""
        bid1   = msg.get("bid1")   or msg.get("bq1")  or msg.get("매수호가1") or ""
        av1    = msg.get("ask1_qty") or msg.get("av1") or msg.get("매도호가수량1") or ""
        bv1    = msg.get("bid1_qty") or msg.get("bv1") or msg.get("매수호가수량1") or ""

        return {
            "timestamp": now,
            "trnm": msg.get("trnm", ""),
            "symbol": str(symbol).zfill(6) if str(symbol).isdigit() else str(symbol),
            "fid_10": str(price),
            "fid_11": str(change),
            "fid_12": str(pct),
            "fid_27": str(qty),
            "fid_28": str(tvol),
            "fid_41": str(ask1),
            "fid_51": str(bid1),
            "fid_61": str(av1),
            "fid_71": str(bv1),
            "raw": json.dumps(msg, ensure_ascii=False),
        }

# -----------------------------------------------------------------------------
# GUI
# -----------------------------------------------------------------------------
class DataLoggerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TickSutra — Kiwoom REST Logger")
        self.setGeometry(120, 120, 860, 600)

        self.token_mgr = TokenManager(APP_KEY, APP_SECRET)
        self.ws_client = KiwoomWSClient(self.token_mgr)
        self.parquet   = ParquetBuffer(BUFFER_SIZE, OUTPUT_DIR)

        self._logging_active = False

        cw = QWidget(); self.setCentralWidget(cw)
        layout = QVBoxLayout(cw)

        self.lbl_conn = QLabel("상태: 연결 안됨")
        self.lbl_buf  = QLabel("버퍼: 0/{}".format(BUFFER_SIZE))
        self.lbl_rec  = QLabel("총 레코드: 0")
        layout.addWidget(self.lbl_conn); layout.addWidget(self.lbl_buf); layout.addWidget(self.lbl_rec)

        row = QHBoxLayout()
        self.btn_connect = QPushButton("연결(토큰→WS)")
        self.btn_start   = QPushButton("로깅 시작")
        self.btn_stop    = QPushButton("로깅 중지")
        row.addWidget(self.btn_connect); row.addWidget(self.btn_start); row.addWidget(self.btn_stop)
        layout.addLayout(row)

        layout.addWidget(QLabel("로그:"))
        self.log_box = QTextEdit(); self.log_box.setReadOnly(True); self.log_box.setMaximumHeight(320)
        layout.addWidget(self.log_box)

        # Signals
        self.btn_connect.clicked.connect(self._connect)
        self.btn_start.clicked.connect(self._start)
        self.btn_stop.clicked.connect(self._stop)
        self.ws_client.connected.connect(self._on_ws_connected)
        self.ws_client.status.connect(self._log)
        self.ws_client.data.connect(self._on_data)
        self.parquet.buffer_flushed.connect(self._on_flush)
        self.parquet.error.connect(self._log)

        self._timer = QTimer(); self._timer.timeout.connect(self._tick); self._timer.start(1000)

    def _log(self, msg: str):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_box.append(f"[{t}] {msg}")
        sb = self.log_box.verticalScrollBar(); sb.setValue(sb.maximum())

    # Actions
    def _connect(self):
        try:
            _ = self.token_mgr.get()
            self._log("토큰 발급 완료")
            self.ws_client.connect()
        except Exception as e:
            self._log(f"연결 실패: {e}")

    def _start(self):
        # If you need per-항목 items/symbols, set them here.
        # Example for symbols-based feeds: self.ws_client.set_subscription(["005930","000660"], ["<add-type>"])
        self._logging_active = True
        self._log(f"로깅 시작 (types={SUBSCRIBE_TYPES}, items={SUBSCRIBE_ITEMS})")

    def _stop(self):
        self._logging_active = False
        try: self.parquet.stop()
        except Exception: pass
        self._log("로깅 중지")

    # Callbacks
    def _on_ws_connected(self, ok: bool):
        self.lbl_conn.setText("상태: ✅ 연결됨" if ok else "상태: ❌ 끊김")

    def _on_data(self, rec: Dict[str, Any]):
        if self._logging_active:
            self.parquet.add(rec)

    def _on_flush(self, n: int, path: str):
        self._log(f"💾 {n:,} rows -> {Path(path).name}")

    def _tick(self):
        self.lbl_buf.setText(f"버퍼: {self.parquet.active_len}/{BUFFER_SIZE}")
        self.lbl_rec.setText(f"총 레코드: {self.parquet.total:,}")

    def closeEvent(self, e):
        try: self.parquet.stop()
        except Exception: pass
        e.accept()

def main():
    if "YOUR_APP_KEY" in APP_KEY or "YOUR_APP_SECRET" in APP_SECRET:
        print("Set KIWOOM_APP_KEY / KIWOOM_APP_SECRET environment variables first.")
        sys.exit(1)
    app = QApplication(sys.argv)
    w = DataLoggerGUI(); w.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
