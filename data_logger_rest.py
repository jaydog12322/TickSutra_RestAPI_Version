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
    QPushButton, QLabel, QTextEdit, QFileDialog
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
# Default to 실시간 종목 체결(0B) + 호가잔량(0D) so trade/호가 정보가 모두 수신됩니다.
SUBSCRIBE_TYPES   = ["0B", "0D"]
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
        self._reg_sent = False

    def apply_subscription(self) -> None:
        if self.ws and self._connected:
            try:
                self._send_reg()
            except Exception as e:
                self.status.emit(f"REG 전송 실패: {e}")
        else:
            self.status.emit("WS 연결 후 구독이 적용됩니다.")

    def apply_subscription(self) -> None:
        if self.ws and self._connected:
            try:
                self._send_reg()
            except Exception as e:
                self.status.emit(f"REG 전송 실패: {e}")
        else:
            self.status.emit("WS 연결 후 구독이 적용됩니다.")

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
        self._reg_sent = False
        self.connected.emit(False)
        self.status.emit("WS closed")

    def _on_error(self, *_):
        self._reg_sent = False
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
                "type":  self._types,   # e.g., ["0B", "0D"] 기본; 필요 시 다른 실시간 타입 추가
            }]
        }
        self.ws.send(json.dumps(frame))
        self._reg_sent = True
        self.status.emit(f"REG sent: items={self._items} types={self._types}")

    def _map_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Kiwoom 실시간 응답을 fid_* 스키마로 정리합니다."""

        def _merge_body(container: Any, out: Dict[str, Any]) -> None:
            if not container:
                return
            if isinstance(container, dict):
                keys = set(container.keys())
                if "fid" in keys and "value" in keys:
                    out[str(container.get("fid"))] = container.get("value")
                    return
                if "key" in keys and "value" in keys:
                    out[str(container.get("key"))] = container.get("value")
                    return
                if "field" in keys and "data" in keys:
                    out[str(container.get("field"))] = container.get("data")
                    return
                if "name" in keys and "value" in keys:
                    out[str(container.get("name"))] = container.get("value")
                    return
                for k, v in container.items():
                    if isinstance(v, (dict, list)):
                        _merge_body(v, out)
                    else:
                        out[str(k)] = v
            elif isinstance(container, list):
                for entry in container:
                    if isinstance(entry, dict):
                        _merge_body(entry, out)
                    else:
                        out[str(entry)] = entry

        body_map: Dict[str, Any] = {}
        for key in ("body", "msgBody", "msg_body", "output", "output1", "rt_data"):
            _merge_body(msg.get(key), body_map)

        now = datetime.now().isoformat()
        trnm = str(msg.get("trnm", "")).upper()

        def _pick(*candidates: Any) -> str:
            for cand in candidates:
                if cand is None:
                    continue
                key = str(cand)
                if key in body_map and body_map[key] not in (None, ""):
                    return str(body_map[key])
                if key in msg and msg[key] not in (None, ""):
                    return str(msg[key])
            return ""

        symbol_candidates = [
            msg.get("tr_key"),
            msg.get("symbol"),
            msg.get("code"),
            body_map.get("tr_key"),
            body_map.get("symbol"),
            body_map.get("code"),
            body_map.get("종목코드"),
            body_map.get("stck_shrn_iscd"),
        ]
        symbol = next((s for s in symbol_candidates if s), "")
        if isinstance(symbol, (int, float)) and not isinstance(symbol, bool):
            symbol = f"{int(symbol):06d}"
        else:
            symbol = str(symbol).strip()
            symbol = symbol.zfill(6) if symbol.isdigit() else symbol

        record: Dict[str, Any] = {
            "trnm": trnm,
            "symbol": symbol,
        }
        base_fids = ["10", "11", "12", "13", "15", "27", "28"]
        for fid in base_fids:
            record[f"fid_{fid}"] = _pick(fid)

        # Top 10 price levels (공통 호가)
        for fid in list(range(41, 61)) + list(range(61, 81)):
            record[f"fid_{fid}"] = _pick(fid)

        # 거래소별 호가 잔량(KRX/NXT)
        for fid in range(6044, 6054):
            record[f"fid_{fid}"] = _pick(fid)
        for fid in range(6054, 6064):
            record[f"fid_{fid}"] = _pick(fid)
        for fid in range(6066, 6076):
            record[f"fid_{fid}"] = _pick(fid)
        for fid in range(6076, 6086):
            record[f"fid_{fid}"] = _pick(fid)

        # Allow direct Korean-name fallbacks for 주요 항목
        if not record.get("fid_10"):
            record["fid_10"] = _pick("현재가", "stck_prpr")
        if not record.get("fid_12"):
            record["fid_12"] = _pick("등락률", "prdy_ctrt")
        if not record.get("fid_13"):
            record["fid_13"] = _pick("누적거래량", "acml_vol")
        if not record.get("fid_15"):
            record["fid_15"] = _pick("체결량", "cntg_vol")

        record["raw"] = json.dumps(msg, ensure_ascii=False)
        return record
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
        self.ticker_file_path: Optional[str] = None
        self._active_items: List[str] = SUBSCRIBE_ITEMS[:]

        cw = QWidget(); self.setCentralWidget(cw)
        layout = QVBoxLayout(cw)

        self.lbl_conn = QLabel("상태: 연결 안됨")
        self.lbl_buf  = QLabel("버퍼: 0/{}".format(BUFFER_SIZE))
        self.lbl_rec  = QLabel("총 레코드: 0")
        layout.addWidget(self.lbl_conn); layout.addWidget(self.lbl_buf); layout.addWidget(self.lbl_rec)

        row = QHBoxLayout()
        self.btn_connect = QPushButton("연결(토큰→WS)")
        self.btn_file = QPushButton("티커 파일 선택")
        self.btn_start   = QPushButton("로깅 시작")
        self.btn_stop    = QPushButton("로깅 중지")
        row.addWidget(self.btn_connect); row.addWidget(self.btn_file); row.addWidget(self.btn_start); row.addWidget(self.btn_stop)
        layout.addLayout(row)

        self.lbl_file = QLabel("선택된 파일: (없음)")
        self.lbl_items = QLabel(self._format_items_label(self._active_items))
        layout.addWidget(self.lbl_file)
        layout.addWidget(self.lbl_items)

        layout.addWidget(QLabel("로그:"))
        self.log_box = QTextEdit(); self.log_box.setReadOnly(True); self.log_box.setMaximumHeight(320)
        layout.addWidget(self.log_box)

        # Signals
        self.btn_connect.clicked.connect(self._connect)
        self.btn_file.clicked.connect(self._select_file)
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

    def _format_items_label(self, items: List[str]) -> str:
        count = len(items)
        if not count:
            return "구독 종목: (없음)"
        preview = ", ".join(items[:5])
        if count > 5:
            preview += ", ..."
        return f"구독 종목: {count}개 ({preview})"

    def _select_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "티커 엑셀 파일 선택",
            str(Path.cwd()),
            "Excel Files (*.xlsx *.xls);;All Files (*)",
        )
        if path:
            self.ticker_file_path = path
            self.lbl_file.setText(f"선택된 파일: {Path(path).name}")
            self._log(f"티커 파일 선택: {path}")

    def _load_tickers_from_excel(self, path: str) -> List[str]:
        df = pd.read_excel(path)
        if df.empty:
            raise ValueError("엑셀 파일에 데이터가 없습니다.")

        preferred = [
            "ticker", "tickers", "symbol", "symbols", "code", "codes",
            "종목코드", "티커"
        ]
        column_map = {str(col).strip().lower(): col for col in df.columns}
        target_col: Optional[str] = None
        for name in preferred:
            key = name.lower()
            if key in column_map:
                target_col = column_map[key]
                break
        if target_col is None:
            for col in df.columns:
                raw_name = str(col).strip()
                if raw_name in preferred:
                    target_col = col
                    break
        if target_col is None:
            target_col = df.columns[0]

        series = df[target_col]
        items: List[str] = []
        for raw in series.dropna():
            value: Any = raw
            if isinstance(value, float):
                if pd.isna(value):
                    continue
                if value.is_integer():
                    value = int(value)
            if isinstance(value, int) and not isinstance(value, bool):
                item = f"{value:06d}"
            else:
                text = str(value).strip()
                if not text:
                    continue
                if text.endswith(".0") and text.replace(".", "", 1).isdigit():
                    text = text[:-2]
                item = text.zfill(6) if text.isdigit() else text
            if item and item not in items:
                items.append(item)

        return items

    # Actions
    def _connect(self):
        try:
            _ = self.token_mgr.get()
            self._log("토큰 발급 완료")
            self.ws_client.connect()
        except Exception as e:
            self._log(f"연결 실패: {e}")

    def _start(self):
        items = SUBSCRIBE_ITEMS[:]
        if self.ticker_file_path:
            try:
                loaded = self._load_tickers_from_excel(self.ticker_file_path)
                if loaded:
                    items = loaded
                    preview = ", ".join(items[:5]) + (", ..." if len(items) > 5 else "")
                    self._log(f"엑셀에서 {len(items)}개 종목 로드: {preview}")
                else:
                    self._log("엑셀 파일에서 유효한 종목코드를 찾지 못했습니다. 기본 설정을 사용합니다.")
            except Exception as e:
                self._log(f"엑셀 파일 읽기 실패: {e}. 기본 설정을 사용합니다.")

        self._active_items = items
        self.lbl_items.setText(self._format_items_label(items))
        self.ws_client.set_subscription(items, SUBSCRIBE_TYPES)
        self.ws_client.apply_subscription()
        self._logging_active = True
        preview = ", ".join(items[:5]) + (", ..." if len(items) > 5 else "") if items else "(없음)"
        self._log(f"로깅 시작 (types={SUBSCRIBE_TYPES}, items={preview})")

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
