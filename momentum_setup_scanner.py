# ============================================================
# CLEAN BOX + HH BREAK SCANNER (1H)
# Binance + MEXC | USDT Spot Only
# Option 1: BULK ticker prefilter (24h quote volume) to avoid dead pairs
# Commands: /ping /status /help
# NO RETEST
# + Network timeout hardening (per-symbol retry + higher ccxt timeout)
# ============================================================

import os
import time
import math
import traceback
from datetime import datetime, timezone

import requests
import ccxt
from ccxt.base.errors import RequestTimeout, NetworkError, ExchangeError
from dotenv import load_dotenv

# ---------------- CONFIG ----------------
TIMEFRAME = "1h"
FETCH_LIMIT = 260

BOX_WINDOWS = list(range(6, 13))
BOX_HIGH_SPREAD_PCT_MAX = 1.10
BOX_DRIFT_PCT_MAX = 1.00

BREAK_BUFFER_PCT = 0.25
MIN_BODY_RATIO = 0.60
MIN_CLOSE_POS = 0.75

HH_LOOKBACK = 20
HH_BUFFER_PCT = 0.20

RV_AVG_LEN = 20
LARGE_CAP_QVOL_USDT = 50_000_000
RV_BREAK_LARGE = 2.0
RV_BREAK_MID = 2.5

SOFT_VOL_FLOOR = 50_000    # 24h quote volume floor for universe building
MIN_QVOL_USDT  = 300_000   # 24h quote volume gate for firing alerts

COOLDOWN_BREAK = 60 * 60 * 3

SLEEP_BETWEEN_SYMBOLS = 0.10
SLEEP_BETWEEN_CYCLES = 25
CHECK_TELEGRAM_EVERY_N_SYMBOLS = 25

# How often to rebuild symbol universe via bulk tickers
REFRESH_UNIVERSE_EVERY_SEC = 60 * 60 * 2  # 2 hours

# Telegram polling
TG_LONGPOLL_TIMEOUT = 5
TG_POLL_SLEEP = 0.35

# Network hardening
CCXT_TIMEOUT_MS = 30_000
OHLCV_RETRY_SLEEP = 0.4

# ---------------- FILTERS ----------------
STABLE_FIAT_BASES = {
    "USDT","USDC","BUSD","TUSD","FDUSD","USDP","DAI","FRAX","USDD","USDE",
    "PYUSD","EURC","EURO","USTC","LUSD","GUSD","SUSD","PAX"
}
DENY_BASE_EXACT = {
    "PALLON","SLVON","ORCLON","XOMON","JDON","FUTUON",
    "NVOON","MCDON","JPMON","COINON","ABBVON","METAON","JNJON"
}
TOKENIZED_KEYWORDS = ("TOKENIZED","STOCK","XSTOCK","SHARE","ETF","RWA")

def looks_like_stock_on(base: str) -> bool:
    b = base.upper()
    if b in DENY_BASE_EXACT:
        return True
    if 5 <= len(b) <= 8 and b.endswith("ON") and b[:-2].isalpha() and b[:-2].isupper():
        return True
    if any(k in b for k in TOKENIZED_KEYWORDS):
        return True
    return False

def looks_like_stock_x(base: str) -> bool:
    b = base.upper()
    if any(k in b for k in TOKENIZED_KEYWORDS):
        return True
    if 4 <= len(b) <= 7 and b.endswith("X") and b[:-1].isalpha() and b[:-1].isupper():
        return True
    return False

def should_exclude(symbol: str) -> bool:
    try:
        base, quote = symbol.split("/")
    except:
        return True
    if quote.upper() != "USDT":
        return True
    if base.upper() in STABLE_FIAT_BASES:
        return True
    if looks_like_stock_on(base):
        return True
    if looks_like_stock_x(base):
        return True
    return False

# ---------------- TELEGRAM ----------------
def tg_send(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=12)
    except:
        pass

def tg_get_updates(offset: int | None):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        return {"ok": False, "result": []}
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {"timeout": TG_LONGPOLL_TIMEOUT}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(url, params=params, timeout=TG_LONGPOLL_TIMEOUT + 8)
        return r.json()
    except:
        return {"ok": False, "result": []}

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def format_uptime(start_ts: float) -> str:
    sec = int(time.time() - start_ts)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h}h {m}m {s}s"

# ---------------- HELPERS ----------------
def sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n

def rv_multiplier(qv_24h: float | None) -> float:
    if qv_24h is None:
        return RV_BREAK_MID
    return RV_BREAK_LARGE if qv_24h >= LARGE_CAP_QVOL_USDT else RV_BREAK_MID

def body_ratio(o,h,l,c):
    rng = max(h-l, 1e-12)
    return abs(c-o) / rng

def close_pos(h,l,c):
    rng = max(h-l, 1e-12)
    return (c-l) / rng

def round_sig(x):
    if x == 0:
        return 0
    return round(x, 6 - int(math.floor(math.log10(abs(x)))) - 1)

def highest_high(highs, n):
    if len(highs) < n:
        return None
    return max(highs[-n:])

def find_box(highs, closes):
    best = None
    for w in BOX_WINDOWS:
        if len(highs) < w + 2:
            continue
        hs = highs[-w:]
        cs = closes[-w:]
        box_high = max(hs)

        high_spread = (max(hs) - min(hs)) / max(hs) * 100.0
        if high_spread > BOX_HIGH_SPREAD_PCT_MAX:
            continue

        drift = abs((cs[-1] - cs[0]) / cs[0] * 100.0)
        if drift > BOX_DRIFT_PCT_MAX:
            continue

        score = high_spread
        if best is None or score < best["score"]:
            best = {"box_high": box_high, "w": w, "score": score}
    return best

# ---------------- STATE ----------------
break_state = {}  # key -> last alert timestamp
tg_last_update_id = 0

def cooldown_ok(key, cooldown):
    last = float(break_state.get(key, 0))
    return (time.time() - last) > cooldown

def mark_break(key):
    break_state[key] = time.time()

# ---------------- EXCHANGES ----------------
def make_exchanges():
    return {
        "KuCoin": ccxt.kucoin({
            "enableRateLimit": True,
            "timeout": CCXT_TIMEOUT_MS,
            "options": {"defaultType": "spot"}
        }),
        "MEXC": ccxt.mexc({
            "enableRateLimit": True,
            "timeout": CCXT_TIMEOUT_MS,
            "options": {"defaultType": "spot"}
        })
    }

def build_universe_from_bulk_tickers(ex):
    """
    - Load markets (active spot)
    - Bulk fetch tickers
    - Keep only USDT pairs passing filters + quoteVolume >= MIN_QVOL_USDT
    Returns: (symbols, qv_map)
    """
    markets = ex.load_markets()

    # Bulk tickers (fastest way to filter liquidity upfront)
    tickers = ex.fetch_tickers()

    qv_map = {}
    symbols = []

    for sym, m in markets.items():
        try:
            if not m.get("active", True):
                continue
            if m.get("spot") is False:
                continue
            if not sym.endswith("/USDT"):
                continue
            if should_exclude(sym):
                continue

            t = tickers.get(sym)
            if not t:
                continue

            qv = t.get("quoteVolume")
            if qv is None:
                bv = t.get("baseVolume")
                last = t.get("last")
                if bv is not None and last is not None:
                    qv = bv * last

            if qv is None:
                continue

            qv = float(qv)
            if qv < SOFT_VOL_FLOOR:
                continue

            qv_map[sym] = qv
            symbols.append(sym)

        except:
            continue

    symbols.sort()
    return symbols, qv_map

# ---------------- COMMAND HANDLER ----------------
def handle_commands(stats):
    global tg_last_update_id

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    offset = tg_last_update_id + 1 if tg_last_update_id else None
    data = tg_get_updates(offset)
    if not data or not data.get("ok"):
        time.sleep(TG_POLL_SLEEP)
        return

    updates = data.get("result", [])
    if not updates:
        time.sleep(TG_POLL_SLEEP)
        return

    for upd in updates:
        try:
            tg_last_update_id = upd.get("update_id", tg_last_update_id)

            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue

            msg_chat_id = str(msg.get("chat", {}).get("id", ""))
            if str(chat_id) != msg_chat_id:
                continue

            text = (msg.get("text") or "").strip()
            if not text.startswith("/"):
                continue

            cmd = text.split()[0].lower()

            if cmd == "/help":
                tg_send("Commands:\n/ping\n/status\n/help")
            elif cmd == "/ping":
                tg_send(f"🟢 Alive.\nLast scan: {stats.get('last_scan_utc','-')}\nCycle: {stats.get('cycle',0)}")
            elif cmd == "/status":
                tg_send(
                    "📊 Scanner Status\n"
                    f"Uptime: {stats.get('uptime','-')}\n"
                    f"Cycle: {stats.get('cycle',0)}\n"
                    f"Last scan: {stats.get('last_scan_utc','-')}\n"
                    f"Symbols: KuCoin {stats.get('kucoin_symbols',0)} | MEXC {stats.get('mexc_symbols',0)}\n"
                    f"Alerts sent: {stats.get('alerts_sent',0)}\n"
                    f"Last symbol: {stats.get('last_symbol','-')}"
                )
        except:
            continue

    time.sleep(TG_POLL_SLEEP)

# ---------------- SIGNAL LOGIC ----------------
def fetch_ohlcv_safe(ex, symbol):
    try:
        return ex.fetch_ohlcv(symbol, TIMEFRAME, limit=FETCH_LIMIT)
    except (RequestTimeout, NetworkError):
        # retry once
        try:
            time.sleep(OHLCV_RETRY_SLEEP)
            return ex.fetch_ohlcv(symbol, TIMEFRAME, limit=FETCH_LIMIT)
        except (RequestTimeout, NetworkError):
            return None
    except ExchangeError:
        return None

def scan_symbol(ex, ex_name, symbol, qv_24h, stats):
    if qv_24h is None or qv_24h < MIN_QVOL_USDT:
        return

    ohlcv = fetch_ohlcv_safe(ex, symbol)
    if not ohlcv or len(ohlcv) < 80:
        return

    candles = ohlcv[:-1]  # CLOSED only

    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols   = [c[5] for c in candles]

    o, h, l, c, v = opens[-1], highs[-1], lows[-1], closes[-1], vols[-1]

    avgv = sma(vols, RV_AVG_LEN)
    if not avgv:
        return
    rv = v / avgv

    body_ok = body_ratio(o, h, l, c) >= MIN_BODY_RATIO
    close_ok = close_pos(h, l, c) >= MIN_CLOSE_POS
    rv_need = rv_multiplier(qv_24h)

    key = f"{ex_name}|{symbol}"

    # Mode 1: BOX BREAK
    box = find_box(highs[:-1], closes[:-1])
    if box:
        lvl = box["box_high"]
        if c > lvl * (1 + BREAK_BUFFER_PCT / 100.0) and body_ok and close_ok and rv >= rv_need:
            if cooldown_ok(key, COOLDOWN_BREAK):
                mark_break(key)
                stats["alerts_sent"] += 1
                dist_pct = (c - lvl) / lvl * 100
                tg_send(
                    f"🚀 BREAK (1H)\n"
                    f"{symbol} — {ex_name}\n"
                    f"Price: {round_sig(c)} | Level: {round_sig(lvl)}\n"
                    f"Ext: +{dist_pct:.2f}% | RV: {rv:.2f}×"
                )
            return

    # Mode 2: HH BREAK
    hh = highest_high(highs[:-1], HH_LOOKBACK)
    if hh and c > hh * (1 + HH_BUFFER_PCT / 100.0) and body_ok and close_ok and rv >= rv_need:
        if cooldown_ok(key, COOLDOWN_BREAK):
            mark_break(key)
            stats["alerts_sent"] += 1
            dist_pct = (c - hh) / hh * 100
            tg_send(
                f"🚀 BREAK (1H)\n"
                f"{symbol} — {ex_name}\n"
                f"Price: {round_sig(c)} | Level: {round_sig(hh)}\n"
                f"Ext: +{dist_pct:.2f}% | RV: {rv:.2f}×"
            )

# ---------------- MAIN ----------------
def main():
    load_dotenv()

    start_ts = time.time()
    last_universe_refresh = 0.0

    stats = {
        "cycle": 0,
        "alerts_sent": 0,
        "last_scan_utc": "-",
        "last_symbol": "-",
        "kucoin_symbols": 0,
        "mexc_symbols": 0,
        "uptime": "-",
    }

    exs = make_exchanges()

    symbols_map = {}
    qv_map = {}

    def refresh_universe():
        nonlocal last_universe_refresh, symbols_map, qv_map
        last_universe_refresh = time.time()

        new_symbols_map = {}
        new_qv_map = {}

        for ex_name, ex in exs.items():
            try:
                syms, qvs = build_universe_from_bulk_tickers(ex)
            except Exception:
                syms, qvs = [], {}
            new_symbols_map[ex_name] = syms
            new_qv_map[ex_name] = qvs

        symbols_map = new_symbols_map
        qv_map = new_qv_map

        stats["kucoin_symbols"] = len(symbols_map.get("KuCoin", []))
        stats["mexc_symbols"] = len(symbols_map.get("MEXC", []))

    refresh_universe()
    tg_send("✅ Break Scanner Started (1H) | KuCoin + MEXC — Type /status")

    while True:
        stats["cycle"] += 1
        stats["uptime"] = format_uptime(start_ts)

        if time.time() - last_universe_refresh >= REFRESH_UNIVERSE_EVERY_SEC:
            try:
                refresh_universe()
            except:
                pass

        handle_commands(stats)

        try:
            sym_counter = 0
            for ex_name, ex in exs.items():
                syms = symbols_map.get(ex_name, [])
                qvs = qv_map.get(ex_name, {})

                for sym in syms:
                    stats["last_symbol"] = f"{ex_name} {sym}"
                    scan_symbol(ex, ex_name, sym, qvs.get(sym), stats)

                    sym_counter += 1
                    if sym_counter % CHECK_TELEGRAM_EVERY_N_SYMBOLS == 0:
                        stats["uptime"] = format_uptime(start_ts)
                        handle_commands(stats)

                    time.sleep(SLEEP_BETWEEN_SYMBOLS)

            stats["last_scan_utc"] = now_utc()
            stats["uptime"] = format_uptime(start_ts)
            handle_commands(stats)

        except Exception:
            print(traceback.format_exc())

        time.sleep(SLEEP_BETWEEN_CYCLES)

if __name__ == "__main__":
    main()
