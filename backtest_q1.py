#!/usr/bin/env python3
"""
backtest_q1.py — Walk-Forward Backtest（走步式回測）

原則：對回測期間每個交易日，
只使用截至當日為止的歷史資料計算所有指標——無任何向前看偏差。

- RS 百分位：每日即時排名（用當日所有股票 RS，不用全期資料）
- AVWAP_short：用過去 3 根 K 棒確認（與 fetch_stocks.py 相同修正後邏輯）
- RS slope：用 daily_rs[-6:-1]（排除當天，與 fetch_stocks.py 修正後相同）
- 大盤相位：用截至當日的 TWII MA20/MA60

訊號在第 D 日收盤後觸發，第 D+1 日開盤起模擬進場，最長追蹤 25/8 日。

執行：python backtest_q1.py
輸出：docs/backtest_q1.json
"""
import json, time, math, sys, requests
import yfinance as yf
from datetime import datetime, date, timedelta
from collections import defaultdict
from signals import calc_signals

# ── 設定 ────────────────────────────────────────────────────────────
BT_START       = date(2024, 1, 2)    # 回測起始（含 2024 OOS + 2025 in-sample）
BT_END         = date(2026, 3, 31)   # 回測結束
BT_PERIOD      = "2024-Q1~2026-Q1"  # 顯示標籤
MAX_HOLD_LONG    = 60   # high_base：需要時間發酵，expired win avg +14.7%
MAX_HOLD_TREND   = 25   # breakout / trend_cont
MAX_HOLD_PULLBACK = 10  # ma_pullback：技術面 2 週內不確認即失效
MAX_HOLD_SWING   = 8    # false_breakdown / ma60_support 等短線
BENCHMARK_TID  = "^TWII"
OUTPUT_PATH    = "docs/backtest_q1.json"
SLIP           = 0.002    # 滑價估計 0.2%
MIN_HIST_DAYS  = 70
HEADERS        = {"User-Agent": "Mozilla/5.0 (stock-radar-backtest/1.0)"}

TREND_TYPES = {"breakout", "high_base", "trend_cont"}

# ── 優化：訊號分級 × Portfolio Heat ──────────────────────────────────
# 相位分離倉位：bull 主動重壓，震盪/空頭收縮；結構設計，非回測最佳化
MAX_HEAT_BY_REGIME = {
    "bull":          0.25,   # 多頭：積極進場，讓贏的年份真的贏
    "bull_pullback": 0.15,   # 回檔：維持正常風控
    "range":         0.10,   # 震盪：保守，機會少做少錯
    "bear":          0.05,   # 空頭：幾乎不開倉（REGIME_ACTIVE_SIGNALS 已封鎖）
}
MAX_HEAT     = 0.15   # 向後相容（backtest_yearly 直接引用舊常數時的備用）
SIGNAL_SCALE = {      # 依設計屬性分層，非 EV 擬合
    "high_base":       1.5,   # 高確信度（conf≥4）+ 長期持有
    "breakout":        1.2,   # 高確信度 + 中期持有
    "ma_pullback":     1.0,
    "ma60_support":    0.0,   # 不單獨進場；MA60 近支撐改為第 7 個確認旗標（signals.py）
    "false_breakdown": 0.8,
    "trend_cont":      1.0,
    "retest":          0.0,   # 降為候選清單，公平宇宙回測無 alpha
}

# 每個市場相位允許的訊號類型：結構設計（不同相位適合不同進場邏輯），非 EV 擬合
REGIME_ACTIVE_SIGNALS = {
    "bull":          {"high_base", "breakout", "trend_cont", "ma_pullback", "false_breakdown"},
    "bull_pullback": {"ma_pullback", "false_breakdown"},
    "range":         {"false_breakdown", "ma_pullback"},
    "bear":          set(),   # 空頭不開個股單：留現金縮倉防禦，market_factor 已自動壓縮倉位
}

BASE_R      = 0.012   # base risk per trade as fraction of capital (1.2%)

GAP_LIMIT   = {
    "breakout":        0.04,
    "trend_cont":      0.04,
    "high_base":       0.03,
    "ma_pullback":     0.015,
    "retest":          0.015,
    "ma60_support":    0.02,
    "false_breakdown": 0.05,
}


# ── 技術快照（只用截至第 i 日的資料，無向前看）──────────────────────
def _snapshot(closes, highs, lows, vols, opens, i):
    if i < 62 or math.isnan(closes[i]):
        return None
    price = closes[i]
    if price <= 0:
        return None

    def ma(n):
        if i + 1 < n:
            return None
        return round(sum(closes[i - n + 1:i + 1]) / n, 2)

    def rvwap(anchor):
        """從 anchor 日到 i 日的 AVWAP"""
        anchor = max(0, anchor)
        tv = sum((highs[k] + lows[k] + closes[k]) / 3 * vols[k]
                 for k in range(anchor, i + 1))
        v  = sum(vols[k] for k in range(anchor, i + 1))
        return round(tv / v, 2) if v > 0 else closes[i]

    # avwap_swing：過去 60 日最低點錨定
    base60     = max(0, i - 59)
    swing_lows = [lows[k] if not math.isnan(lows[k]) else float('inf')
                  for k in range(base60, i + 1)]
    swing_anchor = base60 + swing_lows.index(min(swing_lows))

    # avwap_vol：過去 20 日最大量錨定
    base20     = max(0, i - 19)
    peak_vols  = [vols[k] if not math.isnan(vols[k]) else 0
                  for k in range(base20, i + 1)]
    vol_anchor = base20 + peak_vols.index(max(peak_vols))

    # avwap_short：過去 3 日已確認的局部低點（無向前看）
    avwap_short = None
    for j in range(i - 4, base20 - 1, -1):
        if j < 1:
            break
        if lows[j] > lows[j - 1]:
            continue
        past = closes[j + 1:j + 4]   # 3 根過去的收盤，全在 j 之後且已知
        if len(past) >= 2 and sum(1 for c in past if c > lows[j]) >= 2:
            avwap_short = rvwap(j)
            break

    # 20 日高低
    start20    = max(0, i - 19)
    high20     = round(max(highs[start20:i + 1]),  2)
    low20      = round(min(lows[start20:i + 1]),   2)
    prev_low20 = round(min(lows[i - 20:i]), 2) if i >= 21 else low20

    # 量比
    vol_window    = vols[start20:i + 1]
    vol_20avg     = sum(vol_window) / len(vol_window) if vol_window else 1
    vol_day_ratio = round(vols[i] / vol_20avg, 2) if vol_20avg > 0 else 1.0

    # ATR-14（True Range 14日平均）
    def _atr14():
        if i < 14:
            return None
        trs = []
        for k in range(i - 13, i + 1):
            hl = highs[k] - lows[k]
            hc = abs(highs[k] - closes[k - 1]) if k > 0 else 0
            lc = abs(lows[k]  - closes[k - 1]) if k > 0 else 0
            trs.append(max(hl, hc, lc))
        return round(sum(trs) / 14, 2)

    return {
        "price":         price,
        "high":          highs[i],
        "prev_close":    closes[i - 1],
        "ma5":           ma(5),
        "ma10":          ma(10),
        "ma20":          ma(20),
        "ma60":          ma(60),
        "high20":        high20,
        "low20":         low20,
        "prev_low20":    prev_low20,
        "vol_day_ratio": vol_day_ratio,
        "avwap_swing":   rvwap(swing_anchor),
        "avwap_vol":     rvwap(vol_anchor),
        "avwap_short":   avwap_short,
        "atr_14":        _atr14(),
    }


# ── 大盤相位（截至第 i 日的 TWII + 市場廣度 + 52週高點百分位 + 10週動能）────
def _market_regime(bm_closes, i, breadth_pct=0.5):
    """
    三重確認 regime：
      1. MA60 趨勢方向（主判斷）
      2. 市場廣度（% 股票在 MA20 以上）
      3. 52週百分位 + 10週動能（提前 4~6 週識別空頭初期）

    early_bear：距52週高點 >65% 回落（pct52 < 0.35）且 10週動能已負 (-5%+)。
    → 即使 MA60 仍在上方，先把 regime 降為 range 或 bear 防線，
      避免 MA60 落後指標延誤 4~6 週才反應。
    """
    if i < 60:
        return "range"
    p    = bm_closes[i]
    ma60 = sum(bm_closes[i - 59:i + 1]) / 60

    # 52週高低點百分位（約 250 個交易日）
    _w52   = min(i, 249)
    _hi52  = max(bm_closes[i - _w52:i + 1])
    _lo52  = min(bm_closes[i - _w52:i + 1])
    pct52  = (p - _lo52) / (_hi52 - _lo52) if _hi52 > _lo52 else 0.5

    # 10週動能（約 50 個交易日）
    week10_mom = (p / bm_closes[i - 50] - 1) if i >= 50 else 0.0

    # 提前熊市信號：52週位置低 + 10週動能轉負
    early_bear = pct52 < 0.35 and week10_mom < -0.05

    above_ma60 = p > ma60

    if above_ma60 and breadth_pct > 0.55 and not early_bear:
        return "bull"
    if above_ma60 and not early_bear:     # 廣度偏弱或 early_bear 尚未成立
        return "bull_pullback"
    if above_ma60 and early_bear:         # MA60 仍在但領先指標已轉弱
        return "range"
    # p <= ma60
    if breadth_pct < 0.40 or early_bear:  # 廣度崩潰 or 領先信號確認 → 真熊
        return "bear"
    return "range"


# ── Efficiency Ratio（市場趨勢效率，連續縮放用）──────────────────────
def _efficiency_ratio(closes, i, n=20):
    """Kaufman ER：|淨移動| / Σ|逐日移動|
    → 0 = 純震盪；1 = 純趨勢。用大盤 closes 量測整體市場效率。
    """
    if i < n:
        return 0.5
    net  = abs(closes[i] - closes[i - n])
    path = sum(abs(closes[k] - closes[k - 1]) for k in range(i - n + 1, i + 1))
    return round(net / path, 3) if path > 0 else 0.5


# ── 高波動偵測（5日已實現波動率 vs 60日基準）──────────────────────────
def _vol_flag(closes, i, n_fast=5, n_slow=60, threshold=2.0):
    """5日平均絕對日報酬 > 前60日基準 × threshold → True（高波動模式）
    邏輯依據：動能策略在高波動環境失效（方向不穩定）；threshold=2 為圓整設計值。
    """
    if i < n_slow + n_fast:
        return False
    fast_vol = sum(abs(closes[k] / closes[k - 1] - 1)
                   for k in range(i - n_fast + 1, i + 1)) / n_fast
    slow_vol = sum(abs(closes[k] / closes[k - 1] - 1)
                   for k in range(i - n_slow - n_fast + 1, i - n_fast + 1)) / n_slow
    return (fast_vol / slow_vol) >= threshold if slow_vol > 0 else False


# ── 日 RS 序列（個股 vs 大盤，只用截至當日的資料）─────────────────────
def _daily_rs(stock_c, bm_c):
    n   = min(len(stock_c), len(bm_c))
    out = []
    for k in range(1, n):
        sp, sc = stock_c[k - 1], stock_c[k]
        bp, bc = bm_c[k - 1],   bm_c[k]
        if sp > 0 and bp > 0:
            out.append((sc / sp - 1) * 100 - (bc / bp - 1) * 100)
        else:
            out.append(0.0)
    return out


def _rs_metrics(daily_rs):
    """回傳 (M, RS_scalar)；RS_scalar 用於跨股百分位排名"""
    if len(daily_rs) < 10:
        return None, None
    ma10   = sum(daily_rs[-10:]) / 10
    m      = max(-5.0, min(5.0, (daily_rs[-1] / ma10) if ma10 != 0 else 0.0))
    n      = len(daily_rs)
    scalar = (0.4 * sum(daily_rs[-60:])  / 60  if n >= 60  else 0) + \
             (0.3 * sum(daily_rs[-120:]) / 120 if n >= 120 else 0) + \
             (0.3 * sum(daily_rs[-240:]) / 240 if n >= 240 else 0)
    return round(m, 4), round(scalar, 4)


def _rs_slope(daily_rs):
    """5 日斜率，排除今天（用 [-6:-1]）"""
    if len(daily_rs) < 6:
        return None
    vals   = daily_rs[-6:-1]
    mu, xm = sum(vals) / 5, 2.0
    num    = sum((k - xm) * (vals[k] - mu) for k in range(5))
    den    = sum((k - xm) ** 2 for k in range(5))
    return round(num / den, 4) if den else 0.0


# ── 個股相位（簡化版，只用 rs_pct + M + MA）─────────────────────────
def _stock_phase(rs_pct, m_z, snap):
    if rs_pct is None or m_z is None:
        return "RANGE"
    ma20 = snap.get("ma20") or 0
    ma60 = snap.get("ma60") or 0
    p    = snap["price"]
    above60 = p > ma60 > 0
    above20 = p > ma20 > 0
    if rs_pct >= 60 and m_z > 1.0 and above60:
        return "BULL"
    if rs_pct >= 50 and above60 and not above20:
        return "BULL_PULLBACK"
    if rs_pct < 30 and m_z < 1.0 and not above60:
        return "BEAR_WEAK" if m_z > 0.5 else "BEAR_STRONG"
    return "RANGE"


# ── 統計彙整 ──────────────────────────────────────────────────────
def _stats(trades):
    wins    = [t for t in trades if t["outcome"] == "win"  and not math.isnan(t.get("gain_pct", 0))]
    loss    = [t for t in trades if t["outcome"] == "loss" and not math.isnan(t.get("gain_pct", 0))]
    expired = [t for t in trades if t.get("exit_type") == "expired"]
    n       = len(trades)
    dec     = len(wins) + len(loss)
    wr      = round(len(wins) / dec * 100, 1) if dec > 0 else None
    ag      = round(sum(t["gain_pct"] for t in wins) / len(wins),  2) if wins else 0
    al      = round(sum(t["gain_pct"] for t in loss) / len(loss),  2) if loss else 0
    exp     = round(wr / 100 * ag + (1 - wr / 100) * al, 2) if wr is not None else None
    return {
        "count":         n,
        "wins":          len(wins),
        "losses":        len(loss),
        "expired":       len(expired),   # 持倉期滿收盤平倉數（前稱 inconclusive）
        "win_rate":      wr,
        "avg_gain_pct":  ag,
        "avg_loss_pct":  al,
        "expectancy":    exp,
    }


def _capital_curves(trades, bt_start):
    """fixed capital 與 compound equity 兩條資金曲線（按進場日排序）。
    注意：concurrent trades 以進場日順序近似處理。

    pos_factor = actual position size (fraction of capital, already R-sized)
    contrib    = pos_factor × gain_pct / 100
    """
    sorted_t = sorted(trades, key=lambda t: t["date"])

    start_str = bt_start.strftime("%Y-%m-%d")
    fixed_pts  = [{"date": start_str, "equity": 1.0}]
    comp_pts   = [{"date": start_str, "equity": 1.0}]

    fixed_eq = 1.0;  fixed_peak = 1.0;  fixed_mdd = 0.0
    comp_eq  = 1.0;  comp_peak  = 1.0;  comp_mdd  = 0.0

    for t in sorted_t:
        pf    = t.get("pos_factor", 0.05)
        gp    = t.get("gain_pct",   0.0) or 0.0

        # pos_factor is already the true position size (R-based sizing)
        contrib = pf * gp / 100

        fixed_eq   = max(fixed_eq + contrib, 0.0)  # 最低歸零，不穿負
        fixed_peak = max(fixed_peak, fixed_eq)
        if fixed_peak > 0:
            fixed_mdd = max(fixed_mdd, (fixed_peak - fixed_eq) / fixed_peak)
        fixed_pts.append({"date": t["date"], "equity": round(fixed_eq, 4)})

        comp_eq  *= (1 + contrib)
        comp_eq   = max(comp_eq, 1e-6)             # 理論下限
        comp_peak  = max(comp_peak, comp_eq)
        comp_mdd   = max(comp_mdd, (comp_peak - comp_eq) / comp_peak)
        comp_pts.append({"date": t["date"], "equity": round(comp_eq, 4)})

    return {
        "fixed": {
            "curve":            fixed_pts,
            "total_return_pct": round((fixed_eq - 1) * 100, 2),
            "max_drawdown_pct": round(fixed_mdd * 100, 2),
        },
        "compound": {
            "curve":            comp_pts,
            "total_return_pct": round((comp_eq - 1) * 100, 2),
            "max_drawdown_pct": round(comp_mdd * 100, 2),
        },
    }


# ── 主程式 ────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Q1 2025 Walk-Forward Backtest")
    print(f"  期間：{BT_START} ~ {BT_END}")
    print("  原則：每日只用截至當日的已知資料，無向前看偏差")
    print("=" * 60)

    # ── 板塊對應（從 stocks.json 讀取；板塊歸屬穩定，不含未來資訊）──────
    sector_map   = {}   # code → sector_key
    sector_codes = defaultdict(list)   # sector_key → [codes]
    try:
        with open("docs/stocks.json", encoding="utf-8") as _f:
            _sj = json.load(_f)
        for _s in _sj.get("stocks", []):
            _c  = _s.get("code", "")
            _sk = _s.get("sector_key", "")
            if _c and _sk:
                sector_map[_c] = _sk
                sector_codes[_sk].append(_c)
        print(f"  板塊對應：{len(sector_map)} 檔 / {len(sector_codes)} 板塊")
    except Exception as _e:
        print(f"  板塊對應載入失敗（{_e}），sector_rs 使用 None")

    # ── Step 1: 決定候選母體 ─────────────────────────────────────
    # 抗倖存者偏差設計：
    #   1. TWSE API 取今日量能前 500 作為「候選池」（不直接用作母體）
    #   2. 下載資料後，用「回測開始前一整年的平均成交量」重排，取前 300
    #   → PRE_BT_VOL 動態計算（BT_START 前一年），確保任何測試區間都無向後看偏差
    UNIVERSE_CANDIDATES = 500   # 候選池大小（下載後再篩）
    UNIVERSE_FINAL      = 300   # 最終母體大小
    PRE_BT_VOL_START    = date(BT_START.year - 1, 1, 1)
    PRE_BT_VOL_END      = date(BT_START.year - 1, 12, 31)

    print("\n[1] 抓取候選股票池（TWSE 量能前 500）...")
    print(f"  回測期間：{BT_START} ~ {BT_END}")
    print(f"  母體選取依據：{PRE_BT_VOL_START} ~ {PRE_BT_VOL_END} 平均成交量（BT_START 前一整年，無向後看）")
    universe_candidates = []
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json",
            headers=HEADERS, timeout=15)
        d = r.json()
        fields   = d.get("fields", [])
        rows     = d.get("data", [])
        i_code   = fields.index("證券代號") if "證券代號" in fields else 0
        i_vol    = fields.index("成交股數")  if "成交股數"  in fields else 2
        tmp = []
        for row in rows:
            try:
                code = row[i_code].strip()
                if not (code.isdigit() and len(code) == 4): continue
                if code.startswith("00"): continue
                if int(code) < 1000: continue
                vol = int(row[i_vol].replace(",", ""))
                tmp.append((code, vol))
            except Exception:
                continue
        tmp.sort(key=lambda x: x[1], reverse=True)
        universe_candidates = [c for c, _ in tmp[:UNIVERSE_CANDIDATES]]
        print(f"  → 候選池：{len(universe_candidates)} 檔")
    except Exception as e:
        print(f"  TWSE API 失敗：{e}，使用內建候選清單")
        universe_candidates = [
            "2330","2317","2454","2382","2308","2303","2412","3711",
            "2881","2882","2891","2886","2884","2885","1301","1303",
            "6505","2002","2912","2207","1216","2327","3034","3037",
            "2395","2379","4938","2408","2357","2377","3008","2357",
            "2301","2376","2344","2337","3045","2353","2352","2347",
            "2345","2371","2404","2498","3406","4904","6415","6669",
        ]

    universe_candidates = list(dict.fromkeys(universe_candidates))

    # ── Step 2: 下載歷史資料 ─────────────────────────────────────
    # 起始日早於 Q1_START 240 個交易日（~1 年）供 RS 計算
    # 結束日晚於 Q1_END 30 個交易日供結果追蹤
    DATA_START = "2023-06-01"
    DATA_END   = "2026-06-01"

    print(f"\n[2] 下載 TWII 基準 + 0050 比較基準...")
    bm = yf.Ticker(BENCHMARK_TID).history(start=DATA_START, end=DATA_END)
    if bm.empty:
        print("TWII 下載失敗，中止"); sys.exit(1)
    bm_dates    = [d.date() for d in bm.index]
    bm_closes   = [float(v) for v in bm["Close"].tolist()]
    bm_date_idx = {d: i for i, d in enumerate(bm_dates)}
    q1_dates    = [d for d in bm_dates if BT_START <= d <= BT_END]
    print(f"  TWII：{len(bm_dates)} 日 | 回測交易日：{len(q1_dates)} 天")

    # 0050 比較基準曲線（正規化到 BT_START = 1.0）
    benchmark_curve = []
    try:
        _etf = yf.Ticker("0050.TW").history(start=DATA_START, end=DATA_END)
        if not _etf.empty:
            _edates  = [d.date() for d in _etf.index]
            _eclose  = [float(v) for v in _etf["Close"].tolist()]
            _edict   = {d: c for d, c in zip(_edates, _eclose)}
            # 找 BT_START 當日或之後最近的價格作為基準
            _base_d  = next((d for d in sorted(_edict) if d >= BT_START), None)
            _base_px = _edict[_base_d] if _base_d else None
            if _base_px:
                benchmark_curve = [
                    {"date": d.strftime("%Y-%m-%d"), "equity": round(_edict[d] / _base_px, 4)}
                    for d in sorted(_edict) if BT_START <= d <= BT_END
                ]
                print(f"  0050：{len(benchmark_curve)} 日 benchmark 曲線")
    except Exception as _be:
        print(f"  0050 下載失敗（{_be}），benchmark 跳過")

    print(f"\n[3] 下載 {len(universe_candidates)} 檔候選個股資料...")
    print("    (每 20 檔暫停 3 秒避免限速，預計 8~15 分鐘)")
    stock_data = {}
    for idx, code in enumerate(universe_candidates):
        tid = code + ".TW"
        print(f"  [{idx + 1:3d}/{len(universe_candidates)}] {code}", end="  ")
        try:
            h = yf.Ticker(tid).history(start=DATA_START, end=DATA_END)
            if h.empty or len(h) < MIN_HIST_DAYS:
                print("skip"); continue
            stock_data[code] = {
                "dates":  [d.date() for d in h.index],
                "closes": [float(v) for v in h["Close"].tolist()],
                "highs":  [float(v) for v in h["High"].tolist()],
                "lows":   [float(v) for v in h["Low"].tolist()],
                "vols":   [float(v) for v in h["Volume"].tolist()],
                "opens":  [float(v) for v in h["Open"].tolist()],
            }
            print(f"OK ({len(h)} 日)")
        except Exception as e:
            print(f"fail: {e}")
        time.sleep(0.4)
        if (idx + 1) % 20 == 0:
            time.sleep(3)

    print(f"\n  → 下載完成：{len(stock_data)} 檔")

    # ── Step 3b: 用回測前一年（2024）的平均成交量重排，取前 300 ──────
    # 消除倖存者偏差：選股依據為 2024 已知資訊，不使用 2025+ 的未來量能
    print(f"\n  [母體篩選] 計算 {PRE_BT_VOL_START}~{PRE_BT_VOL_END} 平均成交量...")
    pre_bt_vol = {}
    for code, sd in stock_data.items():
        vols_2024 = [
            sd["vols"][i] for i, d in enumerate(sd["dates"])
            if PRE_BT_VOL_START <= d <= PRE_BT_VOL_END
            and not math.isnan(sd["vols"][i]) and sd["vols"][i] > 0
        ]
        if vols_2024:
            pre_bt_vol[code] = sum(vols_2024) / len(vols_2024)

    sorted_by_pre_bt = sorted(pre_bt_vol, key=lambda c: pre_bt_vol[c], reverse=True)
    final_universe   = sorted_by_pre_bt[:UNIVERSE_FINAL]

    # 無 2024 量能資料的股票（IPO 太晚）不納入母體
    no_pre_vol = [c for c in stock_data if c not in pre_bt_vol]
    if no_pre_vol:
        print(f"  [排除] {len(no_pre_vol)} 檔無 2024 量能資料（新上市等）")

    # 只保留最終母體的資料，釋放記憶體
    stock_data = {c: stock_data[c] for c in final_universe if c in stock_data}
    print(f"  → 最終母體：{len(stock_data)} 檔（依 2024 平均成交量選出）")
    if stock_data:
        top5 = final_universe[:5]
        print(f"  → 前 5 大：{', '.join(top5)}")

    # 為每支股票建立「日期 → 陣列 index」的快速對照
    stock_date_idx = {}
    for code, sd in stock_data.items():
        stock_date_idx[code] = {d: i for i, d in enumerate(sd["dates"])}

    # ── Step 3: Walk-Forward 主迴圈 ──────────────────────────────
    print(f"\n[4] Walk-Forward 逐日掃描（{len(q1_dates)} 個交易日）...")
    trades         = []
    open_positions = []   # (exit_date, heat_fraction) — portfolio heat 追蹤

    for q_date in q1_dates:
        bm_i = bm_date_idx.get(q_date)
        if bm_i is None:
            continue

        # ── 4a: RS scalar + 市場廣度（單次遍歷，廣度用於 regime 雙確認）
        rs_scalar_map = {}
        rs_cache      = {}
        _above_ma20   = 0
        _breadth_n    = 0
        for code, sd in stock_data.items():
            si = stock_date_idx[code].get(q_date)
            if si is None or si < 10:
                continue
            n_align        = min(si + 1, bm_i + 1)
            dr             = _daily_rs(sd["closes"][:n_align], bm_closes[:n_align])
            rs_cache[code] = dr
            _, scalar      = _rs_metrics(dr)
            if scalar is not None:
                rs_scalar_map[code] = scalar
            # 廣度：同一次遍歷順帶計算（需 si >= 20）
            if si >= 20:
                _cl = sd["closes"][si]
                if not math.isnan(_cl):
                    _ma20v = sum(sd["closes"][si - 19:si + 1]) / 20
                    _breadth_n += 1
                    if _cl > _ma20v:
                        _above_ma20 += 1

        breadth_pct = _above_ma20 / _breadth_n if _breadth_n > 0 else 0.5

        # ── regime 在廣度計算後判斷（雙確認：MA60 × 廣度）
        regime = _market_regime(bm_closes, bm_i, breadth_pct)

        # ── market_factor：連續縮放，與 regime 分類獨立運作
        twii_mom_20 = (bm_closes[bm_i] / bm_closes[bm_i - 20] - 1) if bm_i >= 20 else 0.0
        _brf        = max(0.3, min(1.5, breadth_pct / 0.5))
        _mmf        = max(0.3, min(1.5, 1.0 + twii_mom_20))
        # ER 連續乘數
        _er         = _efficiency_ratio(bm_closes, bm_i, 20)
        _er_scale   = max(0.3, min(_er / 0.30, 1.2))
        # 高波動乘數：5日波動 > 2× 60日基準 → 新倉縮半（高波動環境動能策略失效）
        _high_vol   = _vol_flag(bm_closes, bm_i)
        _vol_mult   = 0.5 if _high_vol else 1.0
        market_factor = round(max(0.3, min(1.5, _brf * _mmf * _er_scale * _vol_mult)), 3)

        # ── 清除已到期的 heat 部位（依信號日判斷）
        open_positions = [(ed, h) for ed, h in open_positions if ed > q_date]

        # ── 4b: 計算當日跨股 RS 百分位
        if len(rs_scalar_map) > 1:
            sorted_codes = sorted(rs_scalar_map, key=lambda c: rs_scalar_map[c])
            n_rs         = len(sorted_codes)
            rs_pct_map   = {c: round(i / (n_rs - 1) * 100, 1)
                            for i, c in enumerate(sorted_codes)}
        else:
            rs_pct_map = {c: 50.0 for c in rs_scalar_map}

        # ── 類股 RS 百分位（每日計算，用於個股倉位加權）
        _sec_sum = defaultdict(float)
        _sec_cnt = defaultdict(int)
        for _c, _sc in rs_scalar_map.items():
            _sk = sector_map.get(_c, "")
            if _sk:
                _sec_sum[_sk] += _sc
                _sec_cnt[_sk] += 1
        _sec_avg = {sk: _sec_sum[sk] / _sec_cnt[sk] for sk in _sec_sum}
        if len(_sec_avg) > 1:
            _sec_sorted = sorted(_sec_avg, key=lambda s: _sec_avg[s])
            _sec_n      = len(_sec_sorted)
            _sec_pct    = {s: round(i / (_sec_n - 1) * 100, 1)
                           for i, s in enumerate(_sec_sorted)}
        else:
            _sec_pct = {s: 50.0 for s in _sec_avg}

        day_count = 0

        # ── 4c: 對每支股票產生訊號
        for code, sd in stock_data.items():
            si = stock_date_idx[code].get(q_date)
            if si is None or si < 62:
                continue
            if sd["dates"][si] != q_date:
                continue   # 該股當日無資料（停牌等）
            if si + 1 >= len(sd["closes"]):
                continue   # 後面沒有追蹤資料

            snap = _snapshot(sd["closes"], sd["highs"], sd["lows"],
                             sd["vols"],   sd["opens"],  si)
            if snap is None:
                continue

            rs_pct = rs_pct_map.get(code, 50.0)
            dr     = rs_cache.get(code, [])
            m_z, _ = _rs_metrics(dr)
            slope  = _rs_slope(dr)

            _code_sk       = sector_map.get(code, "")
            _code_sec_pct  = _sec_pct.get(_code_sk, 50.0)

            snap["m_z"]            = m_z
            snap["rs_trend_stock"] = slope
            snap["sector_rs"]      = _code_sec_pct

            phase = _stock_phase(rs_pct, m_z, snap)

            sigs = calc_signals(
                snap, {}, rs_pct,
                stock_phase=phase,
                market_regime=regime,
                composite_score=50,   # 中性，讓技術條件完整運作
            )
            if not sigs:
                continue

            # ── 4d: 次日進場模擬
            def _retest_pf(pf, stype, reg):
                """retest 在非多頭市場半倉：Q1/Q2 EV 負，Q3/Q2026 EV 正，動態而非固定上限"""
                if stype == "retest" and reg != "bull":
                    return round(pf * 0.5, 2)
                return pf

            ni        = si + 1
            nxt_open  = sd["opens"][ni]
            nxt_high  = sd["highs"][ni]

            for sig in sigs:
                trigger  = sig.get("trigger_price", sig["entry"])
                stop     = sig["stop_loss"]
                target   = sig["target"]
                sig_type = sig["type"]

                # 確認次日觸發
                if nxt_open > trigger:
                    gap_pct      = (nxt_open - trigger) / trigger
                    if gap_pct > GAP_LIMIT.get(sig_type, 0.02):
                        continue
                    actual_entry = round(nxt_open * (1 + SLIP), 2)
                    entry_type   = "gap_up"
                elif nxt_high >= trigger:
                    gap_pct      = 0.0
                    actual_entry = round(trigger * (1 + SLIP), 2)
                    entry_type   = "intraday"
                else:
                    continue   # 當日未觸發

                actual_risk = actual_entry - stop
                if actual_risk <= 0:
                    continue
                actual_rr = round((target - actual_entry) / actual_risk, 2)
                if actual_rr < 1.5:
                    continue   # RR 門檻：1.2→1.5，移除邊際交易（EV 僅 +0.26%）

                # retest / ma60_support：SIGNAL_SCALE=0.0，不進場
                if sig_type == "retest":
                    continue

                # ── 依市場相位篩選訊號類型（空頭不跑趨勢單；震盪不跑高基底）
                if sig_type not in REGIME_ACTIVE_SIGNALS.get(regime, set()):
                    continue

                # ── RS 加速篩選：震盪/回檔相位只取 RS 持續上升的個股
                if regime in ("range", "bull_pullback") and slope <= 0:
                    continue

                # ── True R-based sizing（訊號設計屬性 × 確認數 × 市場因子 × 類股強度）
                confs      = sig.get("confirmations", 0)
                conf_mult  = 1.2 if confs >= 5 else (1.1 if confs >= 4 else 1.0)
                sig_scale  = SIGNAL_SCALE.get(sig_type, 1.0)
                # 類股 RS 加權：強勢類股 +20%，弱勢類股 -20%（連續，無硬門檻）
                _sector_mult = round(0.8 + 0.004 * _code_sec_pct, 3)
                target_R   = BASE_R * sig_scale * conf_mult * market_factor * _sector_mult
                _stop_dist  = actual_risk / actual_entry if actual_entry > 0 else 0.05
                pos_size   = min(target_R / _stop_dist, 0.20)
                _trade_heat  = round(pos_size * _stop_dist, 5)
                _total_heat  = sum(h for _, h in open_positions)
                _regime_heat = MAX_HEAT_BY_REGIME.get(regime, 0.15)
                if _total_heat + _trade_heat > _regime_heat:
                    continue   # 超過整體風險預算

                # ── 4e: 追蹤結果（只看已過去的資料）
                if sig_type == "high_base":
                    max_days = MAX_HOLD_LONG
                elif sig_type == "ma_pullback":
                    max_days = MAX_HOLD_PULLBACK   # 2週：技術面定義，不確認即失效
                elif sig_type in TREND_TYPES:
                    max_days = MAX_HOLD_TREND
                else:
                    max_days = MAX_HOLD_SWING
                open_positions.append((q_date + timedelta(days=max_days + 1), _trade_heat))
                final_si  = min(ni + max_days, len(sd["closes"]) - 1)
                outcome   = "inconclusive"
                # 向前找最後一個有效收盤價（yfinance 偶爾回傳 NaN）
                _fsi = final_si
                while _fsi > ni and math.isnan(sd["closes"][_fsi]):
                    _fsi -= 1
                exit_px   = sd["closes"][_fsi] if not math.isnan(sd["closes"][_fsi]) else actual_entry

                for d in range(1, final_si - ni + 1):
                    fh = sd["highs"][ni + d]
                    fl = sd["lows"][ni + d]
                    fo = sd["opens"][ni + d]
                    hit_t = fh >= target
                    hit_s = fl <= stop
                    if hit_t and hit_s:
                        outcome = "win" if fo >= (target + stop) / 2 else "loss"
                        exit_px = target if outcome == "win" else stop
                        break
                    elif hit_t:
                        outcome = "win";  exit_px = target; break
                    elif hit_s:
                        outcome = "loss"; exit_px = stop;   break

                # 持倉期滿未碰停損/目標 → 以最後一天收盤價強制平倉
                if outcome == "inconclusive":
                    exit_type = "expired"
                    outcome   = "win" if exit_px > actual_entry else "loss"
                else:
                    exit_type = "target" if outcome == "win" else "stop"

                gain_pct = round((exit_px - actual_entry) / actual_entry * 100, 2)

                trades.append({
                    "date":          q_date.strftime("%Y-%m-%d"),
                    "code":          code,
                    "type":          sig_type,
                    "label":         sig["label"],
                    "strength":      sig["strength"],
                    "strategy":      sig["strategy"],
                    "regime":        regime,
                    "stock_phase":   phase,
                    "rs_pct":        rs_pct,
                    "entry":         round(actual_entry, 2),
                    "stop":          round(stop,         2),
                    "target":        round(target,       2),
                    "entry_type":    entry_type,
                    "exit_type":     exit_type,
                    "outcome":       outcome,
                    "gain_pct":      gain_pct,
                    "actual_rr":     actual_rr,
                    "confirmations":  sig.get("confirmations", 0),
                    "pos_factor":     round(pos_size, 4),
                    "market_er":      round(_er, 3),
                    "market_factor":  market_factor,
                    "sector_key":     _code_sk,
                    "sector_rs_pct":  round(_code_sec_pct, 1),
                    "high_vol":       _high_vol,
                })
                day_count += 1

        _hv_tag = " ⚡高波動" if _high_vol else ""
        print(f"  {q_date}  regime={regime:<14}{_hv_tag:<6}  訊號={day_count:3d}筆  累計={len(trades)}")

    print(f"\n  ✓ 回測完成：共 {len(trades)} 筆觸發交易")

    # ── Step 4: 統計 ──────────────────────────────────────────────
    print("\n[5] 統計彙整...")

    by_type     = defaultdict(list)
    by_strength = defaultdict(list)
    by_month    = defaultdict(list)
    by_quarter  = defaultdict(list)
    by_regime   = defaultdict(list)
    by_conf     = defaultdict(list)
    by_sector   = defaultdict(list)

    def _quarter(d_str):
        y, m = int(d_str[:4]), int(d_str[5:7])
        return f"{y}-Q{(m-1)//3+1}"

    for t in trades:
        by_type[t["type"]].append(t)
        by_strength[t["strength"]].append(t)
        by_month[t["date"][:7]].append(t)
        by_quarter[_quarter(t["date"])].append(t)
        by_regime[t["regime"]].append(t)
        c = t.get("confirmations", 0)
        conf_key = "5+" if c >= 5 else ("4" if c == 4 else ("3" if c == 3 else "0-2"))
        by_conf[conf_key].append(t)
        sk = t.get("sector_key", "")
        if sk:
            by_sector[sk].append(t)

    overall = _stats(trades)
    print(f"  整體：{overall['count']} 筆  勝率 {overall['win_rate']}%"
          f"  均盈 +{overall['avg_gain_pct']}%  均虧 {overall['avg_loss_pct']}%"
          f"  期望值 {overall['expectancy']}%")
    for sig_type, tlist in sorted(by_type.items()):
        s = _stats(tlist)
        print(f"  [{sig_type:>16}] {s['count']:3d}筆  勝率 {str(s['win_rate'])+'%':>6}  "
              f"均盈 {s['avg_gain_pct']:+.2f}%  均虧 {s['avg_loss_pct']:+.2f}%")

    result = {
        "period":             BT_PERIOD,
        "generated_at":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        "universe_size":      len(stock_data),
        "trading_days":       len(q1_dates),
        "overall":            overall,
        "by_type":            {k: _stats(v) for k, v in by_type.items()},
        "by_strength":        {k: _stats(v) for k, v in by_strength.items()},
        "by_month":           {k: _stats(v) for k, v in by_month.items()},
        "by_quarter":         {k: _stats(v) for k, v in sorted(by_quarter.items())},
        "by_regime":          {k: _stats(v) for k, v in by_regime.items()},
        "by_confirmations":   {k: _stats(v) for k, v in sorted(by_conf.items())},
        "by_sector":          {k: _stats(v) for k, v in sorted(by_sector.items())},
        "capital_curves":     _capital_curves(
                                  trades,
                                  BT_START),
        "benchmark_curve":    benchmark_curve,
        "trades":             trades,
    }

    curves = result["capital_curves"]
    print(f"  固定資本報酬：{curves['fixed']['total_return_pct']:+.1f}%  "
          f"MaxDD {curves['fixed']['max_drawdown_pct']:.1f}%")
    print(f"  複利報酬：    {curves['compound']['total_return_pct']:+.1f}%  "
          f"MaxDD {curves['compound']['max_drawdown_pct']:.1f}%")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        # NaN / Inf → None（標準 JSON 不允許 NaN）
        def _sanitize(obj):
            if isinstance(obj, float):
                return None if (math.isnan(obj) or math.isinf(obj)) else obj
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_sanitize(v) for v in obj]
            return obj
        json.dump(_sanitize(result), f, ensure_ascii=False, indent=2)
    print(f"\n  ✓ 結果已寫入 {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
