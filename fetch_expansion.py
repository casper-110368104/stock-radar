"""
隨機500股訊號掃描器
資料來源：
  - TWSE STOCK_DAY_ALL：全市場股票 + 流動性篩選（免費無限制）
  - yfinance：OHLC 歷史、均線計算（免費無需 token）
不使用 FinMind / Gemini，完全不消耗付費 API 額度。
"""

import requests, json, time, os, random, sys
import yfinance as yf
from datetime import datetime


def get_json_with_retry(url, headers, timeout=20, retries=4, backoff=5):
    """帶重試的 GET JSON，記錄狀態供診斷"""
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, headers=headers, timeout=timeout)
            print(f"  [HTTP] status={res.status_code} len={len(res.content)} bytes (attempt {attempt})")
            if res.status_code != 200:
                raise ValueError(f"HTTP {res.status_code}")
            if not res.content:
                raise ValueError("empty response body")
            return res.json()
        except Exception as e:
            print(f"  [retry {attempt}/{retries}] {e}")
            if attempt < retries:
                wait = backoff * attempt
                print(f"  等待 {wait}s 後重試...")
                time.sleep(wait)
    return None

OUTPUT_PATH  = "docs/expansion.json"
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; stock-radar-bot/1.0)"}
MIN_PRICE    = 15          # 最低股價（排除低價股）
SAMPLE_SIZE  = 500         # 每次隨機抽取數量
ETF_PREFIXES = ("00",)

_bm_closes_exp = []  # 基準指數收盤序列（^TWII）供 RS 計算用


# ── 1. TWSE 全市場資料 ──────────────────────────────────────
def fetch_all_twse_stocks():
    """抓 TWSE STOCK_DAY_ALL，回傳流動性足夠的所有股票"""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
    # stat 非 OK 時（資料尚未更新）最多等 4 次，每次間隔 30s
    d = None
    for stat_attempt in range(1, 5):
        d = get_json_with_retry(url, HEADERS, timeout=20, retries=4, backoff=5)
        if d is None:
            print("  [TWSE] 多次重試後仍失敗")
            return []
        if d.get("stat") == "OK":
            break
        print(f"  [TWSE] 狀態異常: {d.get('stat')} (attempt {stat_attempt}/4)")
        if stat_attempt < 4:
            print("  等待 30s 後再次嘗試取得資料...")
            time.sleep(30)
    else:
        print("  [TWSE] 4 次嘗試後 stat 仍非 OK，放棄")
        return []
    try:
        fields = d.get("fields", [])
        rows   = d.get("data", [])
        print(f"  [TWSE] fields: {fields}")   # 診斷用：確認欄位名稱

        # 嘗試定位各欄索引，相容新舊欄位名
        def find(candidates, fallback):
            for c in candidates:
                if c in fields:
                    return fields.index(c)
            return fallback

        i_code = find(["證券代號", "股票代號"], 0)
        i_name = find(["證券名稱", "股票名稱"], 1)
        i_vol  = find(["成交股數", "成交張數", "成交量"], 2)
        i_open = find(["開盤價"], 5)
        i_high = find(["最高價"], 6)
        i_low  = find(["最低價"], 7)
        i_cls  = find(["收盤價"], 8)

        # 判斷成交量單位（張 vs 股）：欄位含「張」或「量」視為以張計
        vol_field = fields[i_vol] if i_vol < len(fields) else ""
        is_lots   = "張" in vol_field or vol_field == "成交量"
        # 門檻：股 ≥ 500,000（約 500 張）；張 ≥ 500
        MIN_VOL = 500 if is_lots else 500_000
        print(f"  [TWSE] 成交量欄位='{vol_field}'，單位={'張' if is_lots else '股'}，門檻={MIN_VOL:,}")

        result = []
        for r in rows:
            try:
                code = r[i_code].strip()
                if not (code.isdigit() and len(code) == 4):
                    continue
                if any(code.startswith(p) for p in ETF_PREFIXES):
                    continue
                if int(code) < 1000:
                    continue

                vol = int(r[i_vol].replace(",", ""))
                cls = float(r[i_cls].replace(",", "")) if r[i_cls] not in ("--", "") else 0
                opn = float(r[i_open].replace(",", "")) if r[i_open] not in ("--", "") else cls
                high = float(r[i_high].replace(",", "")) if r[i_high] not in ("--", "") else cls
                low  = float(r[i_low ].replace(",", "")) if r[i_low ] not in ("--", "") else cls
                chg_pct = round((cls - opn) / opn * 100, 2) if opn > 0 else 0
                name = r[i_name].strip()

                if vol < MIN_VOL or cls < MIN_PRICE:
                    continue

                result.append({
                    "code": code,
                    "name": name,
                    "vol":  vol,
                    "price": cls,
                    "open":  opn,
                    "high":  high,
                    "low":   low,
                    "chg_pct": chg_pct,
                })
            except Exception:
                continue

        print(f"  [TWSE] 流動性篩選後：{len(result)} 檔")
        return result

    except Exception as e:
        print(f"  [TWSE] 例外：{e}")
        return []


# ── 2. 讀現有股票代碼（排除用）+ 抓 TWSE 產業分類 ──────────
def load_existing_codes():
    try:
        with open("docs/stocks.json", "r", encoding="utf-8") as f:
            d = json.load(f)
        codes = set(s["code"] for s in d.get("stocks", []))
        print(f"  [existing] 已有 {len(codes)} 檔將被排除")
        return codes
    except Exception as e:
        print(f"  [existing] 讀取失敗：{e}，不排除任何代碼")
        return set()


def fetch_twse_industry_map():
    """抓 TWSE 本益比表（BWIBBU_ALL），取得全市場股票的產業類別對照表"""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_ALL?response=json"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        d   = res.json()
        fields = d.get("fields", [])
        rows   = d.get("data", [])
        # fields 通常是 ['代號','名稱','殖利率(%)','股利年度','本益比','股價淨值比','財報年/季']
        # 但有些版本沒有產業欄位，改用 BWIBBU_DAY or SFI
        # 嘗試找代號欄位
        try:
            i_code = fields.index("代號")
        except ValueError:
            i_code = 0
        ind_map = {}
        for r in rows:
            if len(r) > i_code:
                ind_map[r[i_code].strip()] = None
        print(f"  [BWIBBU] 取得 {len(ind_map)} 檔代號（無產業欄位，改用備援）")
        return {}   # BWIBBU_ALL 沒有產業欄，回傳空，觸發備援
    except Exception as e:
        print(f"  [BWIBBU] 失敗：{e}")
        return {}


def fetch_twse_industry_map_isin():
    """備援：從 TWSE 上市公司基本資料 API 取得代號→產業對照表
    endpoint: https://opendata.twse.com.tw/v1/opendata/t187ap03_L
    fields 包含 公司代號, 產業類別
    """
    url = "https://opendata.twse.com.tw/v1/opendata/t187ap03_L"
    try:
        rows = get_json_with_retry(url, HEADERS, timeout=20, retries=3, backoff=5)
        if rows is None:
            return {}
        # rows is list of dicts
        ind_map = {}
        for r in rows:
            code = str(r.get("公司代號", "")).strip()
            ind  = str(r.get("產業類別", "")).strip()
            if code and ind:
                ind_map[code] = ind
        print(f"  [ISIN-opendata] 取得 {len(ind_map)} 檔產業對照")
        return ind_map
    except Exception as e:
        print(f"  [ISIN-opendata] 失敗：{e}")
        return {}


# ── 3. yfinance 歷史資料 + 均線計算 ─────────────────────────
def calc_avwap(closes, highs, lows, volumes, anchor_idx):
    """從 anchor_idx 起累積計算 AVWAP（typical price × volume 加權）"""
    if anchor_idx < 0 or anchor_idx >= len(closes):
        return None
    cum_tv, cum_v = 0.0, 0.0
    for i in range(anchor_idx, len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3.0
        cum_tv += tp * volumes[i]
        cum_v  += volumes[i]
    return round(cum_tv / cum_v, 2) if cum_v > 0 else None


def _compute_rs_layers(sc, bc):
    """計算三層 RS 序列（fast=60日、mid=120日、slow=240日）"""
    n = min(len(sc), len(bc))
    sc, bc = sc[-n:], bc[-n:]
    fast, mid, slow = [], [], []
    for t in range(n):
        if t >= 60:
            fast.append((sc[t]/sc[t-60]-1) - (bc[t]/bc[t-60]-1))
        if t >= 120:
            mid.append((sc[t]/sc[t-120]-1) - (bc[t]/bc[t-120]-1))
        if t >= 240:
            slow.append((sc[t]/sc[t-240]-1) - (bc[t]/bc[t-240]-1))
    return fast, mid, slow


def _compute_m_a(rs_fast):
    """計算 M（RS動能）、A（M加速度）、RS_trend（5日斜率），均以 Z-score 標準化"""
    if len(rs_fast) < 12:
        return None, None, None
    rs_ma10 = sum(rs_fast[-10:]) / 10
    m_raw   = rs_fast[-1] - rs_ma10

    # M 序列（近30天）→ 用 M 自己的分布做 Z-score，避免用 RS μ/σ 導致 m_z 永遠負
    m_series = [rs_fast[i] - sum(rs_fast[i-9:i+1])/10
                for i in range(max(10, len(rs_fast)-30), len(rs_fast)) if i >= 9]
    mu_m  = sum(m_series)/len(m_series) if m_series else 0.0
    std_m = (sum((x-mu_m)**2 for x in m_series)/len(m_series))**0.5 if m_series else 0.0
    m_z   = (m_raw - mu_m) / std_m if std_m > 0 else 0.0

    # A = M_today - M_3day_avg（直覺：今日動能相對近3日均值的偏離）
    m_tail = [rs_fast[i] - sum(rs_fast[i-9:i+1])/10
              for i in range(len(rs_fast)-3, len(rs_fast)) if i >= 9]
    a_z = (m_tail[-1] - sum(m_tail)/len(m_tail)) if len(m_tail) >= 3 else None

    if len(rs_fast) >= 5:
        vals   = rs_fast[-5:]
        mu5    = sum(vals)/5
        x_mean = 2.0
        num    = sum((i-x_mean)*(vals[i]-mu5) for i in range(5))
        den    = sum((i-x_mean)**2 for i in range(5))
        rs_trend = round(num/den, 4) if den else 0.0
    else:
        rs_trend = None

    return m_z, a_z, rs_trend


def classify_stock_phase(rs_pct, m_z, a_z, rs_trend, rs_slow_positive=None):
    """依 RS 百分位、M/A Z-score 和 RS_trend 分類個股型態"""
    if rs_pct is None or m_z is None:
        return "RANGE"
    rs_slow_ok = rs_slow_positive if rs_slow_positive is not None else True
    if rs_pct >= 70 and m_z > 0 and (a_z is None or a_z >= 0) and rs_slow_ok:
        return "RANGE" if (rs_trend is not None and rs_trend < 0) else "BULL"
    if rs_pct >= 60 and m_z < 0 and (a_z is None or a_z < 0) and (rs_trend is None or rs_trend > 0):
        return "BULL_PULLBACK"
    if rs_pct < 30 and m_z < 0 and (a_z is None or a_z < 0):
        return "BEAR_STRONG"
    if rs_pct < 50 and m_z > 0:
        return "BEAR_WEAK"
    return "RANGE"


def fetch_yahoo_data(code):
    """抓 2 年 OHLCV，計算 MA5/10/20/60 及量比 + RS 指標"""
    ticker = yf.Ticker(f"{code}.TW")
    try:
        hist = ticker.history(period="2y")
        if hist.empty or len(hist) < 60:
            return None

        closes  = hist["Close"].tolist()
        highs   = hist["High"].tolist()
        lows    = hist["Low"].tolist()
        volumes = hist["Volume"].tolist()
        opens   = hist["Open"].tolist()

        price      = round(closes[-1], 2)
        prev_close = round(closes[-2], 2) if len(closes) >= 2 else price

        def ma(n):
            if len(closes) < n:
                return None
            return round(sum(closes[-n:]) / n, 2)

        high20     = round(max(highs[-20:]),  2)
        low20      = round(min(lows[-20:]),   2)
        prev_low20 = round(min(lows[-21:-1]), 2) if len(lows) >= 21 else low20

        vol_20avg     = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else (volumes[-1] or 1)
        vol_day_ratio = round(volumes[-1] / vol_20avg, 2) if vol_20avg > 0 else 1.0

        # ── Anchored VWAP 三線 ─────────────────────────────
        n = len(closes)

        # avwap_swing：60日最低點，需有結構轉強確認（price>MA20 或 量比>1）
        _base60   = max(0, n - 60)
        idx_swing = _base60 + lows[_base60:].index(min(lows[_base60:]))
        avwap_swing = None
        for _j in range(idx_swing + 1, n):
            _win = closes[max(0, _j-19):_j+1]
            _ma20_j = sum(_win) / len(_win)
            _vwin   = volumes[max(0, _j-19):_j+1]
            _vol20  = sum(_vwin) / len(_vwin) if _vwin else 1
            if closes[_j] > _ma20_j or volumes[_j] / _vol20 > 1:
                avwap_swing = calc_avwap(closes, highs, lows, volumes, idx_swing)
                break

        # avwap_vol：20日最大量那天，需收盤>MA20（排除出貨）
        _base20 = max(0, n - 20)
        idx_vol = _base20 + volumes[_base20:].index(max(volumes[_base20:]))
        _vwin20 = closes[max(0, idx_vol-19):idx_vol+1]
        _ma20_v = sum(_vwin20) / len(_vwin20)
        avwap_vol = calc_avwap(closes, highs, lows, volumes, idx_vol) if closes[idx_vol] > _ma20_v else None

        # avwap_short：最近20日內，最近一個「局部低點+後3日有反彈確認」的 index
        avwap_short = None
        for _j in range(n - 2, _base20 - 1, -1):
            if _j < 1:
                break
            if lows[_j] > lows[_j-1] or lows[_j] > lows[min(_j+1, n-1)]:
                continue
            _ahead = closes[_j+1:min(_j+4, n)]
            if len(_ahead) >= 2 and sum(1 for c in _ahead if c > lows[_j]) >= 2:
                avwap_short = calc_avwap(closes, highs, lows, volumes, _j)
                break

        # ── RS 指標計算 ─────────────────────────────────
        global _bm_closes_exp
        _rs_scalar = None
        m_z_val = a_z_val = rs_trend_val = None
        if _bm_closes_exp:
            try:
                bc = _bm_closes_exp
                sc = closes
                _n = min(len(sc), len(bc))
                if _n >= 240:
                    sc_a = sc[-_n:]; bc_a = bc[-_n:]
                    r60  = sc_a[-1]/sc_a[-60]  - 1; b60  = bc_a[-1]/bc_a[-60]  - 1
                    r120 = sc_a[-1]/sc_a[-120] - 1; b120 = bc_a[-1]/bc_a[-120] - 1
                    r240 = sc_a[-1]/sc_a[-240] - 1; b240 = bc_a[-1]/bc_a[-240] - 1
                    _rs_scalar = round(0.4*(r60-b60) + 0.3*(r120-b120) + 0.3*(r240-b240), 4)
                rs_fast, _, _ = _compute_rs_layers(sc, bc)
                m_z_val, a_z_val, rs_trend_val = _compute_m_a(rs_fast)
            except Exception:
                pass

        return {
            "price":         price,
            "prev_close":    prev_close,
            "ma5":           ma(5),
            "ma10":          ma(10),
            "ma20":          ma(20),
            "ma60":          ma(60),
            "high20":        high20,
            "low20":         low20,
            "prev_low20":    prev_low20,
            "vol_day_ratio": vol_day_ratio,
            # AVWAP 三線
            "avwap_swing":   avwap_swing,
            "avwap_vol":     avwap_vol,
            "avwap_short":   avwap_short,
            # RS 指標
            "_rs_scalar":    _rs_scalar,
            "m_z":           round(m_z_val, 4) if m_z_val is not None else None,
            "a_z":           round(a_z_val, 4) if a_z_val is not None else None,
            "rs_trend":      rs_trend_val,
            # 原始序列供回測用，不寫入 JSON
            "_closes":       closes,
            "_highs":        highs,
            "_lows":         lows,
            "_volumes":      volumes,
            "_opens":        opens,
        }
    except Exception:
        return None


# ── 4. 訊號偵測（純技術面，不依賴籌碼）─────────────────────

_RR_MAP = {
    "BULL":          3.0,
    "BULL_PULLBACK": 2.0,
    "RANGE":         1.5,
    "BEAR_WEAK":     1.5,
    "BEAR_STRONG":   1.0,
}

_ALLOWED_SIGNALS = {
    "BULL":          {"breakout", "false_breakdown", "ma_pullback", "high_base", "retest", "ma60_support"},
    "BULL_PULLBACK": {"ma_pullback", "retest"},
    "RANGE":         {"ma_pullback", "retest", "ma60_support"},
    "BEAR_WEAK":     {"false_breakdown", "retest"},
    "BEAR_STRONG":   {"false_breakdown"},
}


def calc_signals(yahoo, stock_phase="RANGE"):
    signals   = []
    price     = yahoo.get("price")     or 0
    high20    = yahoo.get("high20")
    low20     = yahoo.get("low20")
    prev_low20= yahoo.get("prev_low20")
    prev_close= yahoo.get("prev_close")
    ma5       = yahoo.get("ma5")
    ma10      = yahoo.get("ma10")
    ma20      = yahoo.get("ma20")
    ma60      = yahoo.get("ma60")
    vol_day      = yahoo.get("vol_day_ratio") or 1.0
    avwap_swing  = yahoo.get("avwap_swing")
    avwap_vol    = yahoo.get("avwap_vol")
    avwap_short  = yahoo.get("avwap_short")
    m_z_val      = yahoo.get("m_z")
    rs_trend_val = yahoo.get("rs_trend")

    if not price:
        return []

    # AVWAP 狀態標記
    _trend_ok  = avwap_swing is None or price >= avwap_swing
    _mm_ok     = avwap_vol   is None or price >= avwap_vol
    _short_ok  = avwap_short is None or price >= avwap_short

    # BULL 型態動能額外條件
    _bull_momentum = (stock_phase != "BULL") or (
        m_z_val is not None and m_z_val > 0 and
        rs_trend_val is not None and rs_trend_val > 0
    )

    def _sig(type_, label, strength, entry, stop, reason):
        # 型態篩選
        if type_ not in _ALLOWED_SIGNALS.get(stock_phase, _ALLOWED_SIGNALS["RANGE"]):
            return None
        _strength = strength
        _reason   = reason
        if not _trend_ok:
            _strength = {"strong": "medium", "medium": "weak"}.get(strength, strength)
            _reason   = reason + "；⚠️趨勢破 AVWAP"
        if _mm_ok and avwap_vol:
            _reason = _reason + "；主力未跑✓"
        risk = round(entry - stop, 2) if stop else 0
        if risk <= 0:
            return None
        # 動態 RR
        rr = _RR_MAP.get(stock_phase, 2.0)
        if avwap_swing and price >= avwap_swing:
            rr *= 1.2
        elif avwap_short and price < avwap_short:
            rr *= 0.7
        rr = round(rr, 2)
        target = round(entry + risk * rr, 2)
        return {
            "type":      type_,
            "label":     label,
            "strength":  _strength,
            "entry":     round(entry, 2),
            "stop_loss": round(stop, 2),
            "target":    target,
            "risk":      risk,
            "rr":        rr,
            "reason":    _reason,
        }

    # 1. 突破：收盤突破20日高 + 量比≥1.5 + 短線節奏健康 + BULL動能確認
    if high20 and price > high20 and vol_day >= 1.5 and _short_ok and _bull_momentum:
        s = _sig("breakout", "突破", "strong", price, low20 or price * 0.95,
                 f"收盤({price})突破20日高({high20})，量比{vol_day:.1f}x")
        if s: signals.append(s)

    # 2. 假跌破：昨收 < prev_low20 且今收 > low20
    if low20 and prev_close and prev_low20 and prev_close < prev_low20 and price > low20:
        s = _sig("false_breakdown", "假跌破", "strong", price, round(low20 * 0.98, 2),
                 f"昨收({prev_close})跌破前20日低，今收({price})強力收復")
        if s: signals.append(s)

    # 3. 均線回測：多頭排列 + 收盤距MA20在3%以內
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20:
        dist = (price - ma20) / ma20
        if 0 <= dist <= 0.03:
            s = _sig("ma_pullback", "均線回測", "medium", price, ma20,
                     f"均線多頭排列，收盤({price})回測MA20({ma20})")
            if s: signals.append(s)

    # 4. 強整再突：緊貼20日高（距離≤5%）+ 收盤 > MA5 + 短線節奏健康 + BULL動能確認
    if high20 and ma5 and price > ma5 and _short_ok and _bull_momentum:
        dist = (high20 - price) / high20
        if 0 <= dist <= 0.05:
            s = _sig("high_base", "強整再突", "medium", price,
                     ma10 or round(price * 0.95, 2),
                     f"緊貼20日高({high20})整理，量比{vol_day:.1f}x")
            if s: signals.append(s)

    # 5. 縮量回測：收盤距MA10在2%以內 + 量比<1 + 收盤>MA20
    if ma10 and ma20 and price > ma20:
        dist = abs(price - ma10) / ma10
        if dist <= 0.02 and vol_day < 1.0:
            s = _sig("retest", "縮量回測", "medium", price, ma20,
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})")
            if s: signals.append(s)

    # 6. MA60支撐：收盤距MA60在2%以內
    if ma60:
        dist = (price - ma60) / ma60
        if 0 <= dist <= 0.02:
            s = _sig("ma60_support", "MA60支撐", "weak", price, round(ma60 * 0.97, 2),
                     f"收盤({price})貼近MA60({ma60})")
            if s: signals.append(s)

    return signals


# ── 5. 回測相關函式 ─────────────────────────────────────────

SIGNAL_LABELS = {
    "breakout":        "突破",
    "false_breakdown": "假跌破",
    "ma_pullback":     "均線回測",
    "high_base":       "強整再突",
    "retest":          "縮量回測",
    "ma60_support":    "MA60支撐",
}


def _rolling_vwap(c, h, l, v, i, n):
    """計算第 i 日往前 n 天的滾動 VWAP（作為 AVWAP proxy）"""
    s = max(0, i - n + 1)
    ctv = sum((h[j] + l[j] + c[j]) / 3 * v[j] for j in range(s, i + 1))
    cv  = sum(v[j] for j in range(s, i + 1))
    return round(ctv / cv, 2) if cv > 0 else c[i]


def calc_yahoo_snapshot(closes, highs, lows, volumes, i):
    """計算歷史第 i 日的技術指標快照（與 fetch_yahoo_data 格式一致）"""
    if i < 20:
        return None
    price      = closes[i]
    prev_close = closes[i - 1]

    def ma(n):
        if i + 1 < n:
            return None
        return round(sum(closes[i - n + 1:i + 1]) / n, 2)

    start  = max(0, i - 19)
    high20 = round(max(highs[start:i + 1]), 2)
    low20  = round(min(lows[start:i + 1]),  2)
    prev_low20 = round(min(lows[i - 20:i]), 2) if i >= 21 else low20

    vol_window    = volumes[start:i + 1]
    vol_20avg     = sum(vol_window) / len(vol_window) if vol_window else 1
    vol_day_ratio = round(volumes[i] / vol_20avg, 2) if vol_20avg > 0 else 1.0

    return {
        "price":         round(price, 2),
        "prev_close":    round(prev_close, 2),
        "ma5":           ma(5),
        "ma10":          ma(10),
        "ma20":          ma(20),
        "ma60":          ma(60),
        "high20":        high20,
        "low20":         low20,
        "prev_low20":    prev_low20,
        "vol_day_ratio": vol_day_ratio,
        "avwap_short":   _rolling_vwap(closes, highs, lows, volumes, i, 20),
        "avwap_swing":   _rolling_vwap(closes, highs, lows, volumes, i, 60),
    }


def backtest_one_stock(closes, highs, lows, volumes, opens=None, stock_phase="RANGE"):
    """對單支股票的歷史資料逐日跑訊號偵測，回傳各訊號的結果清單"""
    n = len(closes)
    if n < 77:   # 62（MA60需求）+ 15（評估窗口）
        return []

    results = []
    for i in range(62, n - 15):
        snapshot = calc_yahoo_snapshot(closes, highs, lows, volumes, i)
        if not snapshot:
            continue
        for sig in calc_signals(snapshot, stock_phase=stock_phase):
            entry  = sig["entry"]
            stop   = sig["stop_loss"]
            target = sig["target"]
            if target <= entry or entry <= stop:
                continue

            # 最快第 1 日起，第 5 日先做判斷；5 日未解決繼續等到第 15 日
            # 同日高觸目標且低觸停損：用開盤價判斷方向（開盤 >= 中點 → 先漲 → win）
            outcome   = "inconclusive"
            final_idx = min(i + 15, n - 1)
            for d in range(1, final_idx - i + 1):
                fh = highs[i + d]
                fl = lows[i + d]
                fo = opens[i + d] if opens and (i + d) < len(opens) else closes[i + d]
                hit_t = fh >= target
                hit_s = fl <= stop
                if hit_t and hit_s:
                    mid = (target + stop) / 2
                    outcome = "win" if fo >= mid else "loss"
                    break
                elif hit_t:
                    outcome = "win";  break
                elif hit_s:
                    outcome = "loss"; break

            # 記錄第 5 日實際漲跌幅（不管是否解決）
            day5_close = closes[min(i + 5, n - 1)]
            import math
            if math.isnan(day5_close) or day5_close <= 0:
                continue
            gain_pct   = round((day5_close - entry) / entry * 100, 2)

            results.append({
                "type":     sig["type"],
                "outcome":  outcome,
                "gain_pct": gain_pct,
            })
    return results


def aggregate_backtest_stats(all_results):
    """彙整所有股票的回測結果，回傳各訊號類型的統計"""
    from collections import defaultdict
    buckets = defaultdict(lambda: {
        "wins": 0, "losses": 0, "inconclusive": 0,
        "gain_sum": 0.0, "loss_sum": 0.0,
    })
    for r in all_results:
        t = r["type"]
        buckets[t][r["outcome"] + "s"] = buckets[t].get(r["outcome"] + "s", 0) + 1
        if r["outcome"] == "win":
            buckets[t]["gain_sum"] += r["gain_pct"]
        elif r["outcome"] == "loss":
            buckets[t]["loss_sum"] += r["gain_pct"]

    stats = {}
    for t, b in buckets.items():
        decided = b["wins"] + b["losses"]
        total   = decided + b["inconclusive"]
        if total < 5:
            continue
        stats[t] = {
            "label":             SIGNAL_LABELS.get(t, t),
            "count":             total,
            "win_rate":          round(b["wins"] / decided, 3) if decided > 0 else 0.5,
            "avg_gain_pct":      round(b["gain_sum"] / b["wins"],   2) if b["wins"]   > 0 else 0.0,
            "avg_loss_pct":      round(b["loss_sum"] / b["losses"], 2) if b["losses"] > 0 else 0.0,
            "inconclusive_rate": round(b["inconclusive"] / total,   3) if total > 0 else 0.0,
        }
    return stats


def update_signal_tracking(prev_tracking, today_price_map, today_results, today_str, sector_rotation=None, today_high_map=None, today_low_map=None, today_open_map=None):
    """更新追蹤清單：更新舊記錄狀態，加入今日新訊號，保留最近 60 筆"""
    import copy
    updated = []

    # 更新舊記錄
    for rec in prev_tracking:
        rec = copy.copy(rec)   # 避免修改原始輸入
        if rec.get("status") != "open":
            updated.append(rec)
            continue
        code          = rec["code"]
        current_price = today_price_map.get(code)
        days_held     = rec.get("days_held", 0) + 1

        if current_price is not None:
            rec["current_price"] = current_price
            rec["gain_pct"]      = round((current_price - rec["entry"]) / rec["entry"] * 100, 2)

        rec["days_held"] = days_held

        today_high = (today_high_map or {}).get(code, current_price)
        today_low  = (today_low_map  or {}).get(code, current_price)
        today_open = (today_open_map or {}).get(code, current_price)
        hit_target = today_high is not None and today_high >= rec["target"]
        hit_stop   = today_low  is not None and today_low  <= rec["stop_loss"]
        if hit_target and hit_stop:
            mid = (rec["target"] + rec["stop_loss"]) / 2
            if today_open is not None and today_open >= mid:
                rec["status"] = "win";  rec["resolved_date"] = today_str
            else:
                rec["status"] = "loss"; rec["resolved_date"] = today_str
        elif hit_target:
            rec["status"] = "win";  rec["resolved_date"] = today_str
        elif hit_stop:
            rec["status"] = "loss"; rec["resolved_date"] = today_str
        elif days_held >= 20:
            rec["status"] = "expired"; rec["resolved_date"] = today_str

        updated.append(rec)

    # 加入今日新訊號（重複標注：同代號同類型已有 open 記錄則標記 repeat=True）
    # expansion 每日新增上限：取最強前 20 筆（strong > medium > weak）
    _STRENGTH_ORD = {"strong": 0, "medium": 1, "weak": 2}
    open_keys = {(r["code"], r["type"]) for r in updated if r.get("status") == "open"}

    new_candidates = []
    for stock in today_results:
        code = stock["code"]
        name = stock["name"]
        for sig in stock.get("signals", []):
            new_candidates.append((stock, sig, (code, sig["type"]) in open_keys))

    new_candidates.sort(key=lambda x: _STRENGTH_ORD.get(x[1].get("strength", "weak"), 2))
    new_candidates = new_candidates[:20]   # 每日新增上限 20 筆

    for stock, sig, is_repeat in new_candidates:
        code        = stock["code"]
        name        = stock["name"]
        entry_price = today_price_map.get(code, sig["entry"])
        sk          = stock.get("industry", "")
        sdata       = (sector_rotation or {}).get(sk, {})
        updated.append({
            "code":          code,
            "name":          name,
            "type":          sig["type"],
            "label":         sig["label"],
            "strength":      sig["strength"],
            "trigger_date":  today_str,
            "entry":         sig["entry"],
            "stop_loss":     sig["stop_loss"],
            "target":        sig["target"],
            "status":        "open",
            "repeat":        is_repeat,
            "sector_key":    sk,
            "sector_phase":  sdata.get("sub_phase", ""),
            "current_price": round(entry_price, 2),
            "days_held":     0,
            "gain_pct":      0.0,
            "resolved_date": None,
        })

    # open 排前面（不限筆數），已結算的依結算日降序，保留最近 60 筆
    open_recs   = [r for r in updated if r.get("status") == "open"]
    closed_recs = sorted(
        [r for r in updated if r.get("status") != "open"],
        key=lambda x: x.get("resolved_date") or "",
        reverse=True,
    )
    return open_recs + closed_recs[:60]


# ── 掃描失敗時保留舊資料並標記 ──────────────────────────────
def _write_scan_failed(reason):
    """保留現有 expansion.json 內的 stocks，但標記 scan_failed 供前端顯示警告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    existing = {}
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except Exception:
        pass

    existing["scan_failed"]    = True
    existing["scan_failed_at"] = now
    existing["scan_failed_reason"] = reason
    # 確保 updated_at 保留舊值（不覆蓋），讓前端知道資料是舊的
    if "updated_at" not in existing:
        existing["updated_at"] = now

    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  [scan_failed] 已寫入失敗狀態：{reason}（{now}）")


# ── 主程式 ──────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  隨機500股訊號掃描器")
    print("=" * 50)

    # Step 1: 全市場
    print("\n[1] 抓取 TWSE 全市場資料...")
    all_stocks = fetch_all_twse_stocks()
    if not all_stocks:
        print("  無法取得市場資料，寫入失敗狀態，保留舊資料。")
        _write_scan_failed("TWSE API 無回應或回傳非 JSON 資料")
        sys.exit(0)

    # Step 1b: 產業對照表（先從 stocks.json 建 fallback，再嘗試 opendata API）
    print("\n[1b] 抓取產業分類對照表...")
    industry_map = {}
    try:
        with open("docs/stocks.json", encoding="utf-8") as _f:
            _sj = json.load(_f)
        for _s in _sj.get("stocks", []):
            if _s.get("sector_key"):
                industry_map[_s["code"]] = _s["sector_key"]
        print(f"  [industry] stocks.json fallback：{len(industry_map)} 檔")
    except Exception:
        pass
    # 嘗試從 opendata API 補齊其餘股票
    api_map = fetch_twse_industry_map_isin()
    if api_map:
        before = len(industry_map)
        for code, ind in api_map.items():
            if code not in industry_map:
                industry_map[code] = ind
        print(f"  [industry] opendata API 補充：+{len(industry_map)-before} 檔，共 {len(industry_map)} 檔")

    # Step 2: 補上產業欄位（不排除個股雷達已有的股票，以確保抽樣母體足夠大）
    print("\n[2] 建立候選池...")
    candidates = all_stocks
    for s in candidates:
        s["industry"] = industry_map.get(s["code"], "")
    print(f"  候選池：{len(candidates)} 檔（含個股雷達已追蹤股）")

    if not candidates:
        print("  候選池為空，寫入失敗狀態，保留舊資料。")
        _write_scan_failed("TWSE 篩選後候選池為空")
        sys.exit(0)

    # 載入舊的追蹤清單（掃描前先讀，避免掃描失敗時遺失）
    prev_tracking = []
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            old_json = json.load(f)
        prev_tracking = old_json.get("signal_tracking", [])
        print(f"  [tracking] 讀取舊追蹤清單：{len(prev_tracking)} 筆")
    except Exception:
        pass

    # Step 1c: 預先抓基準指數（^TWII，供 RS 計算）
    print("\n[1c] 抓取基準指數（^TWII）歷史...")
    global _bm_closes_exp
    try:
        _bm = yf.Ticker("^TWII").history(period="2y")
        _bm_closes_exp = _bm["Close"].tolist() if not _bm.empty else []
        print(f"  基準指數：{len(_bm_closes_exp)} 日資料")
    except Exception as e:
        print(f"  基準指數抓取失敗：{e}")
        _bm_closes_exp = []

    # Step 3: 隨機抽樣
    sample_n = min(SAMPLE_SIZE, len(candidates))
    sample   = random.sample(candidates, sample_n)
    print(f"\n[3] 隨機抽取 {sample_n} 檔，開始掃描...\n")

    # Step 4a: 逐股抓取資料（暫不計算訊號，RS 排名需全部掃描後才能排）
    STRENGTH_ORDER = {"strong": 0, "medium": 1, "weak": 2}
    scanned      = []    # 成功抓到資料的暫存
    raw_histories = {}   # code → (closes, highs, lows, volumes, opens) 供回測用
    no_data = 0

    for i, s in enumerate(sample):
        code, name = s["code"], s["name"]
        print(f"  [{i+1:3d}/{sample_n}] {code} {name:<10}", end="  ")

        yahoo = fetch_yahoo_data(code)
        if not yahoo:
            print("skip（無歷史資料）")
            no_data += 1
            time.sleep(0.3)
            continue

        # 取出原始序列供回測，不存入 JSON
        raw_closes  = yahoo.pop("_closes",  [])
        raw_highs   = yahoo.pop("_highs",   [])
        raw_lows    = yahoo.pop("_lows",    [])
        raw_volumes = yahoo.pop("_volumes", [])
        raw_opens   = yahoo.pop("_opens",   [])
        if raw_closes:
            raw_histories[code] = (raw_closes, raw_highs, raw_lows, raw_volumes, raw_opens)

        print("資料✓")
        scanned.append({"code": code, "name": name, "s": s, "yahoo": yahoo})
        time.sleep(0.4)

    # Step 4b: RS 百分位排名（掃描完後才能排）
    print(f"\n[4b] RS 百分位排名（{len(scanned)} 支有資料）...")
    _rs_pairs = [(sc["code"], sc["yahoo"].get("_rs_scalar"))
                 for sc in scanned if sc["yahoo"].get("_rs_scalar") is not None]
    if len(_rs_pairs) > 1:
        _sorted_rs = sorted(x[1] for x in _rs_pairs)
        _n_rs = len(_sorted_rs)
        _rs_pct_map = {
            code: round(sum(1 for x in _sorted_rs if x <= rv) / _n_rs * 100)
            for code, rv in _rs_pairs
        }
    else:
        _rs_pct_map = {}

    # Step 4c: 型態分類 + 訊號計算
    print(f"\n[4c] 型態分類 + 訊號計算...")
    results = []
    no_sig  = 0
    for sc_item in scanned:
        code  = sc_item["code"]
        name  = sc_item["name"]
        s     = sc_item["s"]
        yahoo = sc_item["yahoo"]
        rs_pct = _rs_pct_map.get(code, 50)
        phase  = classify_stock_phase(
            rs_pct,
            yahoo.get("m_z"),
            yahoo.get("a_z"),
            yahoo.get("rs_trend"),
            None,  # rs_slow_positive 不計算（資料太少）
        )
        yahoo["rs_pct"]      = rs_pct
        yahoo["stock_phase"] = phase

        signals = calc_signals(yahoo, stock_phase=phase)
        if not signals:
            no_sig += 1
            continue

        signals.sort(key=lambda x: STRENGTH_ORDER.get(x["strength"], 9))
        labels = [sg["label"] for sg in signals]
        print(f"  {code} {name:<10}  [{phase}] ✓ {len(signals)} 訊號 → {labels}")

        results.append({
            "code":        code,
            "name":        name,
            "price":       yahoo["price"],
            "chg_pct":     s["chg_pct"],
            "vol_ratio":   yahoo["vol_day_ratio"],
            "ma5":         yahoo["ma5"],
            "ma20":        yahoo["ma20"],
            "ma60":        yahoo["ma60"],
            "avwap_swing": yahoo.get("avwap_swing"),
            "avwap_vol":   yahoo.get("avwap_vol"),
            "avwap_short": yahoo.get("avwap_short"),
            "industry":    s.get("industry", ""),
            "rs_pct":      rs_pct,
            "stock_phase": phase,
            "m_z":         yahoo.get("m_z"),
            "rs_trend":    yahoo.get("rs_trend"),
            "signals":     signals,
        })

    print(f"\n  掃描完成：{len(scanned)} 支有資料，{no_sig} 支無訊號，{len(results)} 支有訊號")

    # Step 5: 以最強訊號排序輸出
    results.sort(key=lambda x: (
        min(STRENGTH_ORDER.get(sg["strength"], 9) for sg in x["signals"]),
        -len(x["signals"])
    ))

    # Step 6: 歷史勝率回測
    print(f"\n[6] 計算歷史訊號勝率（{len(raw_histories)} 支股票）...")
    _phase_map_bt = {sc["code"]: sc["yahoo"].get("stock_phase", "RANGE") for sc in scanned}
    all_bt = []
    for code, (cls, hgh, lws, vols, opn) in raw_histories.items():
        sp = _phase_map_bt.get(code, "RANGE")
        all_bt.extend(backtest_one_stock(cls, hgh, lws, vols, opens=opn, stock_phase=sp))
    backtest_stats = aggregate_backtest_stats(all_bt)
    total_samples  = sum(v["count"] for v in backtest_stats.values())
    print(f"  回測樣本：{len(all_bt)} 筆，有效訊號類型：{len(backtest_stats)} 種，總樣本：{total_samples}")

    # Step 7: 更新信號追蹤
    today_str       = datetime.now().strftime("%Y-%m-%d")
    today_price_map = {s["code"]: s["price"]                for s in all_stocks}
    today_high_map  = {s["code"]: s.get("high", s["price"])  for s in all_stocks}
    today_low_map   = {s["code"]: s.get("low",  s["price"])  for s in all_stocks}
    today_open_map  = {s["code"]: s.get("open", s["price"])  for s in all_stocks}
    # 從 stocks.json 借用產業輪動資料（fetch_stocks.py 先於 fetch_expansion.py 執行）
    _sector_rotation = {}
    try:
        with open("docs/stocks.json", encoding="utf-8") as _f:
            _sector_rotation = json.load(_f).get("sector_rotation", {})
    except Exception:
        pass
    signal_tracking = update_signal_tracking(prev_tracking, today_price_map, results, today_str,
                                             sector_rotation=_sector_rotation,
                                             today_high_map=today_high_map,
                                             today_low_map=today_low_map,
                                             today_open_map=today_open_map)
    open_cnt   = sum(1 for r in signal_tracking if r.get("status") == "open")
    closed_cnt = len(signal_tracking) - open_cnt
    print(f"  [tracking] 追蹤中：{open_cnt} 筆 | 已結算：{closed_cnt} 筆")

    output = {
        "updated_at":      datetime.now().strftime("%Y-%m-%d %H:%M"),
        "scan_failed":     False,
        "sample_size":     sample_n,
        "signal_count":    len(results),
        "backtest_stats":  backtest_stats,
        "signal_tracking": signal_tracking,
        "stocks":          results,
    }

    os.makedirs("docs", exist_ok=True)
    # allow_nan=False 確保 NaN/Inf 不寫入 JSON（瀏覽器 JSON.parse 不支援）
    try:
        json_str = json.dumps(output, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except ValueError:
        # 若仍有 NaN，先用 math.isnan 遞迴清理後重試
        import math
        def _clean(obj):
            if isinstance(obj, float):
                return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_clean(v) for v in obj]
            return obj
        json_str = json.dumps(_clean(output), ensure_ascii=False, separators=(",", ":"))
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(json_str)

    print(f"\n{'='*50}")
    print(f"  完成：掃描 {sample_n} 檔")
    print(f"  有訊號：{len(results)} 檔 | 無資料：{no_data} | 無訊號：{no_sig}")
    print(f"  輸出 → {OUTPUT_PATH}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
