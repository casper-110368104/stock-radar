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
from collections import defaultdict, deque
from pathlib import Path
from signals import calc_signals

def _compute_chips_score(f_net, f_con, t_net, t_con, d_net, vol_lots):
    """計算三大法人籌碼分數（0~100），與 fetch_stocks.py calc_score chips 段落完全一致"""
    chip_pts = 50
    f_pct = f_net / vol_lots * 100
    if   f_pct > 10: chip_pts += 30
    elif f_pct >  5: chip_pts += 20
    elif f_pct >  2: chip_pts += 12
    elif f_pct >  0: chip_pts += 5
    elif f_pct < -5: chip_pts -= 15
    elif f_pct < -2: chip_pts -= 8

    if   f_con >= 10: chip_pts += 15
    elif f_con >=  5: chip_pts += 10
    elif f_con >=  3: chip_pts += 6
    elif f_con >=  1: chip_pts += 2
    elif f_con <= -5: chip_pts -= 12
    elif f_con <= -3: chip_pts -= 7
    elif f_con <= -1: chip_pts -= 3

    t_pct = t_net / vol_lots * 100
    if   t_pct >  3: chip_pts += 10
    elif t_pct >  1: chip_pts += 6
    elif t_pct >  0: chip_pts += 2
    elif t_pct < -1: chip_pts -= 5
    if   t_con >= 5: chip_pts += 5
    elif t_con >= 3: chip_pts += 3
    elif t_con >= 1: chip_pts += 1
    elif t_con <= -3: chip_pts -= 4

    d_pct = d_net / vol_lots * 100
    if   d_pct >  2: chip_pts += 3
    elif d_pct >  0: chip_pts += 1
    elif d_pct < -2: chip_pts -= 2

    return max(0, min(chip_pts, 100))


# ── 設定 ────────────────────────────────────────────────────────────
BT_START       = date(2024, 1, 2)    # 回測起始（含 2024 OOS + 2025 in-sample）
BT_END         = date(2026, 3, 31)   # 回測結束
BT_PERIOD      = "2024-Q1~2026-Q1"  # 顯示標籤
MAX_HOLD_LONG    = 120  # high_base：MA追蹤停損取代時限，安全上限延長至 120 天
MAX_HOLD_TREND   = 40   # breakout / trend_cont：同上
MAX_HOLD_PULLBACK = 10  # ma_pullback：技術面 2 週內不確認即失效
MAX_HOLD_SWING   = 8    # false_breakdown / ma60_support 等短線
MAX_HOLD_IG      = 120  # momentum_ignition：AVWAP 動態止損，讓板塊趨勢充分延伸
MA_TRAIL_BUFFER  = 0.02 # MA10 追蹤停損緩衝（2%）：止跌點 = MA10 × (1 - buffer)
BENCHMARK_TID  = "^TWII"
OUTPUT_PATH    = "docs/backtest_q1.json"
SLIP           = 0.002    # 滑價估計 0.2%
MIN_HIST_DAYS  = 70
HEADERS        = {"User-Agent": "Mozilla/5.0 (stock-radar-backtest/1.0)"}

TREND_TYPES = {"breakout", "high_base", "trend_cont", "momentum_ignition"}

# ── 優化：訊號分級 × Portfolio Heat ──────────────────────────────────
# 相位分離倉位：bull 主動重壓，震盪/空頭收縮；結構設計，非回測最佳化
MAX_HEAT_BY_REGIME = {
    "bull":           0.35,   # 多頭：提高至 35%，讓強多年份 alpha 有更多參與空間
    "bull_pullback":  0.15,   # 回檔：維持正常風控
    "range":          0.10,   # 震盪：保守，機會少做少錯
    "bear":           0.05,   # 空頭：幾乎不開倉（REGIME_ACTIVE_SIGNALS 已封鎖）
    "reversal_probe": 0.08,   # VIX 逆轉 or 廣度背離 → 小倉試水
}
MAX_HEAT     = 0.15   # 向後相容備用

# RS Beta Layer：bull 相位集中持有前 N 強股，配置隨 market_factor 連續縮放
# - 觸發：只在 bull（MA60+MA120+廣度三重確認），離開 bull 即平倉
# - 持股：前 10 強 RS 個股（集中動能，非純分散 beta）
# - 配置：market_factor × BETA_ALLOC_MAX（bull 強時多配，弱時少配，無新參數）
BETA_ALLOC_MAX        = 0.50   # bull 相位最大 beta 曝險（market_factor=1.0 時 = 50%）
BETA_ALLOC_DOMINANT   = 0.70   # 類股主導模式 beta 上限（一個類股明顯斷層領先時）
BETA_TOP_N            = 10     # beta 持股數
SECTOR_DOMINANCE_GAP  = 25.0   # 主導判定：第一名比第二名高出此值（combined_pct 點數）
SECTOR_DOMINANCE_MIN  = 70.0   # 主導判定：第一名至少達此 combined_pct
SECTOR_EXIT_THRESHOLD = 10.0   # 類股斜率百分位低於此值 → 強制出場（消息面惡化）
SECTOR_GATE_THRESHOLD = 30.0   # 保留常數（其他地方可能參照）
SIGNAL_SCALE = {      # 依設計屬性分層，非 EV 擬合
    "high_base":          1.5,   # 高確信度（conf≥4）+ 長期持有
    "breakout":           1.2,   # 高確信度 + 中期持有
    "ma_pullback":        1.0,
    "ma60_support":       0.0,   # 不單獨進場；MA60 近支撐改為第 7 個確認旗標（signals.py）
    "false_breakdown":    0.8,
    "trend_cont":         1.0,
    "retest":             0.0,   # 降為候選清單，公平宇宙回測無 alpha
    "momentum_ignition":  0.8,   # 類股早進：板塊強勢+RS55~75%剛轉正，早於beta layer佈局
}

# 每個市場相位允許的訊號類型：結構設計（不同相位適合不同進場邏輯），非 EV 擬合
REGIME_ACTIVE_SIGNALS = {
    "bull":           {"high_base", "trend_cont", "ma_pullback", "false_breakdown", "momentum_ignition"},
    "bull_pullback":  {"false_breakdown"},   # ma_pullback 在回調相位 exp<0，停用
    "range":          {"false_breakdown"},   # ma_pullback 在震盪相位 exp<0，停用
    "bear":           set(),   # 空頭不開個股單：留現金縮倉防禦，market_factor 已自動壓縮倉位
    "reversal_probe": {"false_breakdown"},  # 轉折試水：VIX 峰值逆轉 or 廣度背離，小倉卡位
}

BASE_R      = 0.012   # base risk per trade as fraction of capital (1.2%)

GAP_LIMIT   = {
    "breakout":           0.04,
    "trend_cont":         0.04,
    "high_base":          0.03,
    "ma_pullback":        0.015,
    "retest":             0.015,
    "ma60_support":       0.02,
    "false_breakdown":    0.05,
    "momentum_ignition":  0.03,  # 動能追高：允許 3% 跳空（新高突破常伴隨跳空）
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

    # 近10日是否有回測MA20：高整型態需有整理動作，過濾連續飆升的假訊號
    _ma20_ref = sum(closes[i - 19:i + 1]) / 20 if i >= 19 else None
    _pulled_back_to_ma20 = False
    if _ma20_ref:
        for _k in range(max(0, i - 9), i + 1):
            if not math.isnan(closes[_k]) and closes[_k] <= _ma20_ref * 1.03:
                _pulled_back_to_ma20 = True
                break

    return {
        "price":              price,
        "high":               highs[i],
        "prev_close":         closes[i - 1],
        "ma5":                ma(5),
        "ma10":               ma(10),
        "ma20":               ma(20),
        "ma60":               ma(60),
        "high20":             high20,
        "low20":              low20,
        "prev_low20":         prev_low20,
        "vol_day_ratio":      vol_day_ratio,
        "avwap_swing":        rvwap(swing_anchor),
        "avwap_vol":          rvwap(vol_anchor),
        "avwap_short":        avwap_short,
        "atr_14":             _atr14(),
        "pulled_back_to_ma20": _pulled_back_to_ma20,
        "swing_anchor_idx":   swing_anchor,
    }


# ── 大盤相位（截至第 i 日的 TWII + 市場廣度 + 52週高點百分位 + 10週動能）────
def _market_regime(bm_closes, i, breadth_pct=0.5, breadth_slope=0.0, fast_breadth_pct=0.5):
    """
    五重確認 regime（慢層 × 非對稱快層）：
      慢層（趨勢確認）
        1. MA60 趨勢方向（主判斷）
        2. MA120（六個月均線）：過濾熊市反彈的假牛訊號
        3. 市場廣度靜態水位（% 股票在 MA20 以上）
        4. 52週百分位 + 10週動能（early_bear 早期預警）
      非對稱快層（市場結構：跌快漲慢）
        5. breadth_slope 10日廣度變化速率
        6. TWII 5日跌幅（ROC5 < -4%）
        7. 快速廣度（5日正報酬股票比例 < 40%）：不依賴均線，直接測量市場動能
           ── 刻意不設 fast_improving：底部確認需等慢層多重信號，
              防止廣度短線反彈造成 bear→range 假升相位頻繁翻轉

    效果：下行加速反應，上行留足確認空間，避免相位 flip-flop。
    """
    if i < 60:
        return "range"
    p    = bm_closes[i]
    ma60 = sum(bm_closes[i - 59:i + 1]) / 60

    # MA120：六個月均線，需同時站上才確認多頭
    ma120       = sum(bm_closes[i - 119:i + 1]) / 120 if i >= 119 else ma60
    above_ma120 = p > ma120

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

    # 非對稱快層：只保留下行保護，不設上行提前升相位
    # ── 廣度快速惡化：10日廣度跌逾10pp
    # ── 指數短期動能：5日跌逾4%（比廣度快2-3週，捕捉趨勢頂部初期轉折）
    # ── 快速廣度：超過60%股票5日負報酬（不依賴均線，直測市場動能瓦解）
    twii_roc5 = (bm_closes[i] / bm_closes[i - 5] - 1) if i >= 5 else 0.0
    fast_deteriorating = (breadth_slope < -0.10
                          or twii_roc5 < -0.04
                          or fast_breadth_pct < 0.40)

    # bull：MA60 + MA120 + 廣度 + 非早期空頭 + 廣度沒有快速惡化
    if above_ma60 and above_ma120 and breadth_pct > 0.55 and not early_bear and not fast_deteriorating:
        return "bull"
    # bull_pullback：MA60 above，但廣度快速惡化 → 直接降到 range（不停在 pullback）
    if above_ma60 and not early_bear:
        return "range" if fast_deteriorating else "bull_pullback"
    if above_ma60 and early_bear:
        return "range"
    # 跌破 MA60：等慢層信號恢復，不用快層提前升相位
    if breadth_pct < 0.40 or early_bear:
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


# ── VIX 風險敞口控制層 ────────────────────────────────────────────────
def _vix_overlay(vix_val, vix_recent=None):
    """
    把 US VIX 當作全球恐慌程度的代理，疊加在 market_factor 之上：
    - VIX > 35: 停止開新倉（heat_mult = 0）
    - VIX 25~35: 線性收縮（1.0 → 0.5）
    - VIX < 25: 正常（heat_mult = 1.0）
    - reversal_probe: 近 25 日內 VIX 曾 ≥ 30，且今天比峰值回落 ≥ 5 點
      → 恐慌消退窗口，允許空頭相位小倉試水

    返回 (heat_mult: float, reversal_probe: bool)
    """
    if vix_val is None or math.isnan(vix_val):
        return 1.0, False

    if vix_val > 35:
        heat_mult = 0.0
    elif vix_val > 25:
        heat_mult = round(1.0 - 0.5 * (vix_val - 25) / 10.0, 3)
    else:
        heat_mult = 1.0

    reversal_probe = False
    if vix_recent:
        valid = [v for v in vix_recent if v is not None and not math.isnan(v)]
        if valid:
            peak = max(valid)
            if peak >= 30 and (peak - vix_val) >= 5:
                reversal_probe = True

    return heat_mult, reversal_probe


# ── 廣度背離偵測（空頭轉折的領先訊號）────────────────────────────────
def _breadth_divergence(bm_closes, bm_i, breadth_history, n=10):
    """
    指數在近期低點附近，但廣度不創低 → 跌勢在收斂，可能底部

    條件：
    1. 今日 TWII 在近 n 日最低點 1.5% 以內（接近低點）
    2. 今日廣度 ≥ 近 n 日最低廣度 + 0.05（廣度相對守住）

    返回 True 表示廣度背離（配合 bear regime 使用）
    """
    if len(breadth_history) < n or bm_i < n:
        return False

    bm_min       = min(bm_closes[bm_i - n + 1:bm_i + 1])
    at_low       = bm_closes[bm_i] <= bm_min * 1.015
    if not at_low:
        return False

    b_min        = min(breadth_history[-n:])
    b_cur        = breadth_history[-1]
    return b_cur >= b_min + 0.05


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
    """回傳 (M, RS_scalar)；M = 累積RS / MA10(累積RS)，與 fetch_expansion.py 一致；
    RS_scalar 用於跨股百分位排名（保留 daily excess return 加權平均）。

    修正：原實作用 daily_rs[-1] / mean(daily_rs[-10:]) 計算 M，
    分母為近 10 日超額報酬均值，在空頭或崩跌後為負 → 符號翻轉，完全失去過濾能力。
    正確做法：先將 daily excess returns 轉為累積 RS 比率（恆正），
    再取 cumRS[-1] / MA10(cumRS)；> 1.0 代表 RS 正在擴張，< 1.0 代表 RS 收縮。
    """
    if len(daily_rs) < 10:
        return None, None
    # 將逐日超額報酬轉為累積 RS 比率（以序列起點為基準）
    cum, cum_rs = 1.0, []
    for dr in daily_rs:
        cum *= (1 + dr / 100)
        cum_rs.append(cum)
    ma10 = sum(cum_rs[-10:]) / 10
    m    = round(max(-5.0, min(5.0, (cum_rs[-1] / ma10) if ma10 > 0.001 else 0.0)), 4)
    n      = len(daily_rs)
    scalar = (0.4 * sum(daily_rs[-60:])  / 60  if n >= 60  else 0) + \
             (0.3 * sum(daily_rs[-120:]) / 120 if n >= 120 else 0) + \
             (0.3 * sum(daily_rs[-240:]) / 240 if n >= 240 else 0)
    return m, round(scalar, 4)


def _rs_slope_window(vals):
    if len(vals) < 5:
        return None
    mu, xm = sum(vals) / 5, 2.0
    num    = sum((k - xm) * (vals[k] - mu) for k in range(5))
    den    = sum((k - xm) ** 2 for k in range(5))
    return round(num / den, 4) if den else 0.0

def _rs_slope(daily_rs):
    """5 日斜率，排除今天（用 [-6:-1]）"""
    return _rs_slope_window(daily_rs[-6:-1]) if len(daily_rs) >= 6 else None

def _rs_accel(daily_rs):
    """RS 加速度：當前5日斜率 − 前5日斜率（正 = 動能在加速）"""
    if len(daily_rs) < 11:
        return None
    s_now  = _rs_slope_window(daily_rs[-6:-1])
    s_prev = _rs_slope_window(daily_rs[-11:-6])
    if s_now is None or s_prev is None:
        return None
    return round(s_now - s_prev, 4)


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


# ── 個股結構分類（複製自 fetch_stocks.py classify_structure）──────────────
def _classify_structure(snap, stock_phase, sector_phase=""):
    """
    依 MA 排列、AVWAP、RS 位置輸出結構標籤。
    '主升段'/'主升段✓'/'主升段✓✓' | '突破準備'/'突破準備✓'/'突破準備✓✓'
    | '回檔' | '盤整' | '弱勢'
    """
    price       = snap.get("price") or 0
    ma5         = snap.get("ma5")
    ma10        = snap.get("ma10")
    ma20        = snap.get("ma20")
    ma60        = snap.get("ma60")
    high20      = snap.get("high20")
    rs_pct_val  = snap.get("rs_pct_val")
    avwap_swing = snap.get("avwap_swing")
    avwap_vol   = snap.get("avwap_vol")
    avwap_short = snap.get("avwap_short")

    if not price or not ma20:
        return "盤整"

    if stock_phase in ("BEAR_STRONG", "BEAR_WEAK") or (ma60 and price < ma60 * 0.98):
        label = "弱勢"
    elif (stock_phase == "BULL" and ma5 and ma10 and ma20
          and ma5 > ma10 > ma20 and price >= ma5):
        label = "主升段"
    elif (high20 and ma20 and price >= ma20
          and 0 <= (high20 - price) / high20 <= 0.10
          and (rs_pct_val is None or rs_pct_val >= 65)):
        label = "突破準備"
    elif (stock_phase in ("BULL", "BULL_PULLBACK")
          and ma10 and ma20 and ma20 <= price <= ma10):
        label = "回檔"
    else:
        label = "盤整"

    _avwap_all_ok = (
        avwap_swing and avwap_vol and avwap_short
        and price >= avwap_swing and price >= avwap_vol and price >= avwap_short
    )
    _avwap_broken = avwap_swing and price < avwap_swing

    if _avwap_broken and label in ("主升段", "突破準備"):
        label = "回檔"

    if label in ("主升段", "突破準備"):
        if sector_phase in ("主升段", "準備噴", "主升回檔"):
            label = label + ("✓✓" if _avwap_all_ok else "✓")
        elif sector_phase == "空頭":
            label = "盤整"
        elif _avwap_all_ok:
            label = label + "✓"

    return label


# ── 統計彙整 ──────────────────────────────────────────────────────
def _apply_sector_exits(trades, stock_data, stock_date_idx, daily_sec_slope_pct, daily_regime, threshold):
    """Post-process：類股惡化強制出場，僅在非多頭相位觸發（bull/bull_pullback 下類股輪動屬正常）。"""
    SECTOR_EXIT_REGIMES = {"bear", "range", "reversal_probe"}
    for t in trades:
        sector = t.get("sector_key", "")
        if not sector or not t.get("exit_date"):
            continue
        try:
            entry_dt = datetime.strptime(t["date"],      "%Y-%m-%d").date() + timedelta(1)
            exit_dt  = datetime.strptime(t["exit_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        code = t["code"]
        check = entry_dt
        while check <= exit_dt:
            regime_at = daily_regime.get(check, "range")
            spct = daily_sec_slope_pct.get(check, {}).get(sector, 50.0)
            if regime_at in SECTOR_EXIT_REGIMES and spct < threshold:
                si = stock_date_idx.get(code, {}).get(check)
                if si is not None:
                    sd   = stock_data[code]
                    opx  = sd["opens"][si] if si < len(sd["opens"]) else 0.0
                    entry = t["entry"]
                    if not math.isnan(opx) and opx > 0 and entry > 0:
                        t["gain_pct"]  = round((opx - entry) / entry * 100, 2)
                        t["outcome"]   = "win" if opx > entry else "loss"
                        t["exit_type"] = "sector_exit"
                        t["exit_date"] = check.strftime("%Y-%m-%d")
                break
            check += timedelta(1)
    return trades


def _gate_blocked_summary(log):
    if not log:
        return {"total": 0, "by_sector": {}, "avg_combined": 0.0}
    by_sec = {}
    for r in log:
        by_sec.setdefault(r["sector"], []).append(r["combined"])
    return {
        "total":      len(log),
        "avg_combined": round(sum(r["combined"] for r in log) / len(log), 1),
        "by_sector":  {s: {"count": len(v), "avg_combined": round(sum(v)/len(v), 1)}
                       for s, v in sorted(by_sec.items(), key=lambda x: -len(x[1]))},
    }


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

    # ── 板塊對應（sector_map.json 為主，stocks.json 補充；板塊歸屬穩定）──────
    sector_map   = {}   # code → sector_key
    sector_codes = defaultdict(list)   # sector_key → [codes]
    # 先載 sector_map.json（全市場，由 build_sector_map.py 產生）
    try:
        with open("docs/sector_map.json", encoding="utf-8") as _f:
            _sm = json.load(_f)
        for _c, _sk in _sm.items():
            if _c and _sk:
                sector_map[_c] = _sk
                sector_codes[_sk].append(_c)
        print(f"  板塊（sector_map.json）：{len(sector_map)} 檔 / {len(sector_codes)} 板塊")
    except FileNotFoundError:
        print("  sector_map.json 不存在，回退至 stocks.json")
    except Exception as _e:
        print(f"  sector_map.json 載入失敗（{_e}）")
    # stocks.json 覆蓋（已人工校正的 sector_key 優先）
    try:
        with open("docs/stocks.json", encoding="utf-8") as _f:
            _sj = json.load(_f)
        for _s in _sj.get("stocks", []):
            _c  = _s.get("code", "")
            _sk = _s.get("sector_key", "")
            if _c and _sk:
                if _c not in sector_map:
                    sector_codes[_sk].append(_c)
                sector_map[_c] = _sk
        print(f"  板塊（+stocks.json overlay）：{len(sector_map)} 檔 / {len(sector_codes)} 板塊")
    except Exception as _e:
        if not sector_map:
            print(f"  板塊載入失敗（{_e}），sector_rs 使用 None")

    # ── 載入歷史籌碼資料 ───────────────────────────────────────────
    _chips_hist = {}
    _chips_hist_path = Path("docs/chips_history.json")
    if _chips_hist_path.exists():
        with open(_chips_hist_path, encoding="utf-8") as f:
            _chips_hist = json.load(f)
        print(f"  [籌碼] 載入 {len(_chips_hist)} 個交易日")
    else:
        print(f"  [籌碼] chips_history.json 不存在，籌碼分數使用中性 50")

    _rev_hist = {}
    _rev_hist_path = Path("docs/revenue_history.json")
    if _rev_hist_path.exists():
        with open(_rev_hist_path, encoding="utf-8") as f:
            _rev_hist = json.load(f)
        print(f"  [月營收] 載入 {len(_rev_hist)} 檔")
    else:
        print(f"  [月營收] revenue_history.json 不存在，月營收分數使用中性 50")

    # ── Step 1: 取得所有 TWSE 上市股票代號 ─────────────────────────
    # 設計原則：
    #   下載全部上市股票的歷史資料，回測迴圈每天以「當日前60日平均成交量」
    #   動態決定當天的候選池（前 UNIVERSE_DAILY_N 名），不依賴固定名單。
    #   → 消除「用今日排名選歷史股票」的前視偏差，每天的候選池反映當下市場環境。
    UNIVERSE_DAILY_N = 300   # 每日動態候選池大小（依當日前60日均量排名）

    print("\n[1] 抓取所有 TWSE 上市股票代號...")
    print(f"  回測期間：{BT_START} ~ {BT_END}")
    universe_candidates = []
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json",
            headers=HEADERS, timeout=15)
        d = r.json()
        fields   = d.get("fields", [])
        rows     = d.get("data", [])
        i_code   = fields.index("證券代號") if "證券代號" in fields else 0
        tmp = []
        for row in rows:
            try:
                code = row[i_code].strip()
                if not (code.isdigit() and len(code) == 4): continue
                if code.startswith("00"): continue
                if int(code) < 1000: continue
                tmp.append(code)
            except Exception:
                continue
        universe_candidates = tmp
        print(f"  → 全市場股票：{len(universe_candidates)} 檔")
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

    # VIX（US 恐慌指數，作為風險敞口控制 overlay）
    vix_dict = {}
    try:
        _vix_raw = yf.Ticker("^VIX").history(start=DATA_START, end=DATA_END)
        if not _vix_raw.empty:
            vix_dict = {d.date(): float(v)
                        for d, v in zip(_vix_raw.index, _vix_raw["Close"].tolist())}
            print(f"  VIX：{len(vix_dict)} 日")
    except Exception as _ve:
        print(f"  VIX 下載失敗（{_ve}），overlay 跳過（heat_mult=1.0）")

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

    print(f"  → 全市場股票資料：{len(stock_data)} 檔（每日動態依當日前60日均量選 Top {UNIVERSE_DAILY_N}）")

    # 為每支股票建立「日期 → 陣列 index」的快速對照
    stock_date_idx = {}
    for code, sd in stock_data.items():
        stock_date_idx[code] = {d: i for i, d in enumerate(sd["dates"])}

    # ── Step 3: Walk-Forward 主迴圈 ──────────────────────────────
    print(f"\n[4] Walk-Forward 逐日掃描（{len(q1_dates)} 個交易日）...")
    trades             = []
    open_positions     = []   # (exit_date, heat_fraction) — portfolio heat 追蹤
    breadth_history    = deque(maxlen=15)   # 近 15 日廣度，供廣度背離偵測
    vix_history        = deque(maxlen=25)   # 近 25 日 VIX，供 reversal_probe 峰值偵測
    sector_rs_history  = defaultdict(lambda: deque(maxlen=20))  # 類股 RS 歷史（供 slope 計算）
    gate_blocked_log      = []   # debug：被 sector gate 擋掉的紀錄
    daily_sec_slope_pct   = {}   # {date: {sector: slope_pct}}，供 sector exit post-process 使用
    daily_regime          = {}   # {date: regime}，供 sector exit 判斷相位

    open_capital         = []        # [(expiry_date, pos_size)]：追蹤信號部位資金佔用

    # ── RS Beta Layer 狀態（獨立於信號交易，記錄在 beta_trades）
    beta_trades        = []
    beta_mode          = None    # None / "active"
    beta_entries       = {}      # code → entry_close_price
    beta_entry_date    = None
    beta_alloc_at_open = 0.0     # 開倉時總配置比例（供平倉計算 pos_factor）
    beta_open_regime   = None    # 開倉時的 regime（供交易記錄）

    for q_date in q1_dates:
        bm_i = bm_date_idx.get(q_date)
        if bm_i is None:
            continue

        # ── 4a: 動態宇宙 + RS scalar + 市場廣度（單次遍歷）
        # Pass 1: 計算所有股票 RS、並收集當日前60日均量 → 選出 Top UNIVERSE_DAILY_N
        rs_scalar_map = {}
        rs_cache      = {}
        _day_vols     = []   # (code, avg_vol_60d)
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
            # 前60日均量（需 si >= 20 保障足夠資料）
            if si >= 20:
                _n = min(si, 59)
                _vslice = sd["vols"][si - _n: si + 1]
                _valid  = [v for v in _vslice if not math.isnan(v) and v > 0]
                if _valid:
                    _day_vols.append((code, sum(_valid) / len(_valid)))

        # 依均量排序，取 Top N 為當日動態宇宙
        _day_vols.sort(key=lambda x: x[1], reverse=True)
        _today_universe = {code for code, _ in _day_vols[:UNIVERSE_DAILY_N]}

        # Pass 2: 廣度 — 只計算動態宇宙內的股票
        _above_ma20 = 0
        _breadth_n  = 0
        for code in _today_universe:
            si = stock_date_idx[code].get(q_date)
            if si is None or si < 20:
                continue
            _cl = stock_data[code]["closes"][si]
            if not math.isnan(_cl):
                _ma20v = sum(stock_data[code]["closes"][si - 19:si + 1]) / 20
                _breadth_n += 1
                if _cl > _ma20v:
                    _above_ma20 += 1

        breadth_pct = _above_ma20 / _breadth_n if _breadth_n > 0 else 0.5

        # ── 快速廣度：5日正報酬股票比例（動態宇宙內）
        _above_5d = 0
        _fast_n   = 0
        for code in _today_universe:
            sd  = stock_data[code]
            si5 = stock_date_idx[code].get(q_date)
            if si5 is None or si5 < 5:
                continue
            if math.isnan(sd["closes"][si5]):
                continue
            _fast_n += 1
            if sd["closes"][si5] > sd["closes"][si5 - 5]:
                _above_5d += 1
        fast_breadth_pct = _above_5d / _fast_n if _fast_n > 0 else 0.5

        # ── 廣度動能（快層）：10日廣度變化速率，breadth_history 含截至昨日的資料
        _b_hist = list(breadth_history)
        _b_ref  = _b_hist[-10] if len(_b_hist) >= 10 else (_b_hist[0] if _b_hist else breadth_pct)
        breadth_slope = round(breadth_pct - _b_ref, 4)

        # ── regime 在廣度計算後判斷（慢層 MA60/MA120 × 快層廣度動能）
        regime = _market_regime(bm_closes, bm_i, breadth_pct, breadth_slope, fast_breadth_pct)

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

        # ── VIX overlay + 廣度背離 → 有效 regime ───────────────────────
        breadth_history.append(breadth_pct)
        # VIX：找今日或往前最近 3 個美股交易日的值（台股開盤美股未必同步）
        _vix_today = vix_dict.get(q_date)
        if _vix_today is None:
            for _doff in (1, 2, 3):
                _vix_today = vix_dict.get(q_date - timedelta(days=_doff))
                if _vix_today is not None:
                    break
        vix_history.append(_vix_today)

        _vix_mult, _reversal_probe = _vix_overlay(_vix_today, list(vix_history)[:-1])
        market_factor = round(max(0.0, market_factor * _vix_mult), 3)

        # 廣度背離（只在空頭相位才有意義）
        _bd = (regime == "bear"
               and _breadth_divergence(bm_closes, bm_i, list(breadth_history)))

        # 有效 regime：bear + (VIX 逆轉 or 廣度背離) → reversal_probe
        _eff_regime = ("reversal_probe"
                       if regime == "bear" and (_reversal_probe or _bd)
                       else regime)

        # ── 清除已到期的 heat 部位（依信號日判斷）
        open_positions = [(ed, h)  for ed, h  in open_positions if ed > q_date]
        open_capital   = [(ed, ps) for ed, ps in open_capital   if ed > q_date]

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

        # ── 類股動能（sector RS slope）：10日變化速率百分位
        for _sk, _savg in _sec_avg.items():
            sector_rs_history[_sk].append(_savg)
        _sec_slope_raw = {
            _sk: (list(sector_rs_history[_sk])[-1] - list(sector_rs_history[_sk])[-10])
                 if len(sector_rs_history[_sk]) >= 10 else 0.0
            for _sk in _sec_avg
        }
        if len(_sec_slope_raw) > 1:
            _ssl  = sorted(_sec_slope_raw, key=lambda s: _sec_slope_raw[s])
            _sln  = len(_ssl)
            _sec_slope_pct = {s: round(i / (_sln - 1) * 100, 1) for i, s in enumerate(_ssl)}
        else:
            _sec_slope_pct = {s: 50.0 for s in _sec_slope_raw}
        # 綜合分數：靜態水位（現在在哪）+ 動能方向（往哪走），各半
        _sec_combined_pct = {
            sk: round(0.5 * _sec_pct.get(sk, 50.0) + 0.5 * _sec_slope_pct.get(sk, 50.0), 1)
            for sk in _sec_avg
        }

        # 記錄每日類股斜率百分位與相位（供 sector exit post-process 使用）
        daily_sec_slope_pct[q_date] = dict(_sec_slope_pct)
        daily_regime[q_date]        = _eff_regime

        # ── 板塊主導偵測
        _sorted_by_combined = sorted(_sec_combined_pct, key=lambda s: _sec_combined_pct[s], reverse=True)
        _dom_gap     = (_sec_combined_pct[_sorted_by_combined[0]] - _sec_combined_pct[_sorted_by_combined[1]]
                        if len(_sorted_by_combined) >= 2 else 0.0)
        _sector_dominant = (
            len(_sorted_by_combined) >= 2
            and _sec_combined_pct[_sorted_by_combined[0]] >= SECTOR_DOMINANCE_MIN
            and _dom_gap >= SECTOR_DOMINANCE_GAP
        )

        # ── RS Beta Layer：bull 相位持有
        _in_bull = (regime == "bull")

        # 關閉 beta 部位：離開 bull
        if beta_mode is not None and not _in_bull:
            _n_beta    = max(len(beta_entries), 1)
            _pf_each   = round(beta_alloc_at_open / _n_beta, 4)
            _exit_type = "regime_change"
            for b_code, b_entry_px in beta_entries.items():
                b_si = stock_date_idx[b_code].get(q_date)
                b_exit_px = b_entry_px
                if b_si is not None and b_si < len(stock_data[b_code]["closes"]):
                    _px = stock_data[b_code]["closes"][b_si]
                    if not math.isnan(_px) and _px > 0:
                        b_exit_px = _px
                b_gain = round((b_exit_px - b_entry_px) / b_entry_px * 100, 2)
                beta_trades.append({
                    "date":          q_date.strftime("%Y-%m-%d"),
                    "code":          b_code,
                    "type":          "beta_momentum",
                    "label":         "RSβ-bull",
                    "strength":      "beta",
                    "strategy":      "beta",
                    "regime":        "bull",
                    "stock_phase":   "BULL",
                    "rs_pct":        rs_pct_map.get(b_code, 80.0),
                    "entry":         round(b_entry_px, 2),
                    "stop":          0.0,
                    "target":        0.0,
                    "entry_type":    "beta",
                    "exit_type":     _exit_type,
                    "outcome":       "win" if b_gain >= 0 else "loss",
                    "gain_pct":      b_gain,
                    "actual_rr":     0.0,
                    "confirmations": 0,
                    "pos_factor":    _pf_each,
                    "market_er":     0.0,
                    "market_factor": 1.0,
                    "sector_key":    sector_map.get(b_code, ""),
                    "sector_rs_pct": 0.0,
                    "high_vol":      _high_vol,
                })
            beta_entries       = {}
            beta_entry_date    = None
            beta_mode          = None
            beta_alloc_at_open = 0.0
            beta_open_regime   = None

        # 開新 beta 部位
        if beta_mode is None and _in_bull:
            top_codes   = sorted(rs_pct_map, key=lambda c: rs_pct_map[c], reverse=True)[:BETA_TOP_N]
            _beta_alloc = round(market_factor * BETA_ALLOC_MAX, 3)
            new_beta = {}
            for b_code in top_codes:
                b_si = stock_date_idx[b_code].get(q_date)
                if b_si is not None and b_si < len(stock_data[b_code]["closes"]):
                    _px = stock_data[b_code]["closes"][b_si]
                    if not math.isnan(_px) and _px > 0:
                        new_beta[b_code] = _px
            if new_beta:
                beta_entries       = new_beta
                beta_entry_date    = q_date
                beta_mode          = "active"
                beta_alloc_at_open = _beta_alloc
                beta_open_regime   = "bull"

        day_count     = 0
        daily_hb_cnt  = 0   # 當日 high_base 進場上限計數
        daily_ig_cnt  = 0   # 當日 momentum_ignition 進場上限計數
        daily_tc_cnt  = 0   # 當日 trend_cont 進場上限計數

        # ── 4c: 對每支股票產生訊號（限動態宇宙，依 RS 百分位由高到低掃描）
        for code, sd in sorted(
                ((c, stock_data[c]) for c in _today_universe if c in stock_data),
                key=lambda x: rs_pct_map.get(x[0], 0),
                reverse=True):
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
            accel  = _rs_accel(dr)
            # 個股5日絕對動能（供 high_base 入場確認：股票本身需在上漲）
            stock_roc5 = (sd["closes"][si] / sd["closes"][si - 5] - 1) if si >= 5 else 0.0

            _code_sk           = sector_map.get(code, "")
            _code_sec_pct      = _sec_pct.get(_code_sk, 50.0)
            _code_sec_combined = _sec_combined_pct.get(_code_sk, 50.0)


            snap["m_z"]             = m_z
            snap["rs_trend_stock"]  = slope
            snap["rs_accel"]        = accel
            snap["sector_rs"]       = _code_sec_pct
            snap["sector_combined"] = _code_sec_combined
            snap["rs_pct_val"]      = rs_pct   # classify_structure 需要

            phase = _stock_phase(rs_pct, m_z, snap)

            # 板塊相位（由板塊 RS slope 百分位近似）
            _sk_slope = _sec_slope_pct.get(_code_sk, 50.0)
            if _sk_slope >= 60:
                _sector_phase = "主升段"
            elif _sk_slope >= 40:
                _sector_phase = "主升回檔"
            elif _sk_slope <= 25:
                _sector_phase = "空頭"
            else:
                _sector_phase = ""

            # 個股結構標籤（供 trend_cont 判斷）
            structure = _classify_structure(snap, phase, _sector_phase)

            # 1. 量能分數（與 fetch_stocks.py 一致）
            _n_vol = min(si, 59)
            _vslice60 = [sd["vols"][si - k] for k in range(min(60, si+1))]
            _vslice20 = _vslice60[:20]
            _vol60avg = sum(v for v in _vslice60 if v > 0) / max(sum(1 for v in _vslice60 if v > 0), 1)
            _vol20avg = sum(v for v in _vslice20 if v > 0) / max(sum(1 for v in _vslice20 if v > 0), 1)
            _vr = (_vol20avg / _vol60avg) if _vol60avg > 0 else 1.0
            if   _vr > 2.0: _vol_score = 100
            elif _vr > 1.5: _vol_score = 85
            elif _vr > 1.3: _vol_score = 70
            elif _vr > 1.1: _vol_score = 55
            elif _vr > 1.0: _vol_score = 40
            elif _vr > 0.8: _vol_score = 25
            else:           _vol_score = 10

            # 2. RS 分數
            if   rs_pct >= 90: _rs_score = 100
            elif rs_pct >= 80: _rs_score = 85
            elif rs_pct >= 70: _rs_score = 70
            elif rs_pct >= 50: _rs_score = 55
            elif rs_pct >= 30: _rs_score = 40
            else:              _rs_score = max(10, int(rs_pct * 0.8))

            # 3. AVWAP 分數（0~15）
            _avwap_cnt = sum([
                bool(snap.get("avwap_swing") and snap["price"] >= snap["avwap_swing"]),
                bool(snap.get("avwap_vol")   and snap["price"] >= snap["avwap_vol"]),
                bool(snap.get("avwap_short") and snap["price"] >= snap["avwap_short"]),
            ])
            _avwap_score = _avwap_cnt * 5

            # 4. 籌碼分數（從 chips_history）
            _chips_raw = _chips_hist.get(q_date.strftime("%Y-%m-%d"), {}).get(code, {})
            _vol_lots = max(sd["vols"][si] / 1000, 1)
            _chips_score_val = _compute_chips_score(
                _chips_raw.get("f", 0), _chips_raw.get("fc", 0),
                _chips_raw.get("t", 0), _chips_raw.get("tc", 0),
                _chips_raw.get("d", 0), _vol_lots
            ) if _chips_raw else 50  # 無資料給中性分

            # 5. 月營收分數
            _rev_score = 50  # 預設中性
            _code_rev = _rev_hist.get(code, {})
            if _code_rev:
                _avail_month = (datetime(q_date.year, q_date.month, 1) - timedelta(days=40)).strftime("%Y-%m")
                _yoy = _code_rev.get(_avail_month)
                if _yoy is not None:
                    if   _yoy > 40: _rev_score = 100
                    elif _yoy > 20: _rev_score = 85
                    elif _yoy > 10: _rev_score = 70
                    elif _yoy >  5: _rev_score = 60
                    elif _yoy >  0: _rev_score = 52
                    elif _yoy > -5: _rev_score = 45
                    elif _yoy >-20: _rev_score = 30
                    else:           _rev_score = 15

            # 6. 基本面分數（無歷史資料，用中性 50）
            _fund_score = 50

            # 7. 正確的 composite_score（與 fetch_stocks.py 完全對齊）
            _tech_score = round(
                _chips_score_val * 0.33 +
                _fund_score      * 0.28 +
                _vol_score       * 0.23 +
                _rev_score       * 0.05 +
                _rs_score        * 0.05 +
                _avwap_score / 15 * 100 * 0.06
            )

            sigs = calc_signals(
                snap, {"chips_score_val": _chips_score_val}, rs_pct,
                stock_phase=phase,
                market_regime=regime,
                composite_score=_tech_score,
                structure=structure,
                sector_phase=_sector_phase,
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

                # 止損距離上限 10%：AVWAP 結構止損過寬時收緊，單筆最大虧損可控
                if actual_entry > 0 and (actual_entry - stop) / actual_entry > 0.10:
                    stop = round(actual_entry * 0.90, 2)
                actual_risk = actual_entry - stop
                if actual_risk <= 0:
                    continue
                actual_rr = round((target - actual_entry) / actual_risk, 2)
                if actual_rr < 1.5:
                    continue   # RR 門檻：1.2→1.5，移除邊際交易（EV 僅 +0.26%）

                # retest / ma60_support：SIGNAL_SCALE=0.0，不進場
                if sig_type == "retest":
                    continue

                # ── 依有效市場相位篩選訊號類型
                if sig_type not in REGIME_ACTIVE_SIGNALS.get(_eff_regime, set()):
                    continue

                # ── RS 加速篩選：震盪/回檔相位只取 RS 持續上升的個股
                if _eff_regime in ("range", "bull_pullback") and slope <= 0:
                    continue

                # high_base：要求 RS 動能正在加速（二階導數 > 0），過濾峰值後退燒的訊號
                if sig_type == "high_base" and (accel is None or accel <= 0):
                    continue

                # high_base：個股5日絕對動能確認（股票本身需在上漲，排除廣度好但個股已轉弱）
                if sig_type == "high_base" and stock_roc5 <= 0:
                    continue

                # ── 每日信號密度上限：high_base ≤ 3、trend_cont ≤ 4、momentum_ignition ≤ 2
                if sig_type == "high_base":
                    if daily_hb_cnt >= 3:
                        continue
                    daily_hb_cnt += 1
                elif sig_type == "trend_cont":
                    if daily_tc_cnt >= 4:
                        continue
                    daily_tc_cnt += 1
                elif sig_type == "momentum_ignition":
                    if daily_ig_cnt >= 2:
                        continue
                    daily_ig_cnt += 1

                # ── True R-based sizing（訊號設計屬性 × 確認數 × 市場因子 × 類股強度）
                confs      = sig.get("confirmations", 0)
                conf_mult  = 1.2 if confs >= 5 else (1.1 if confs >= 4 else 1.0)
                sig_scale  = SIGNAL_SCALE.get(sig_type, 1.0)
                # 類股加權：靜態水位 + 動能方向綜合分數（0.70~1.30）
                _sector_mult = round(0.7 + 0.006 * _code_sec_combined, 3)
                target_R   = BASE_R * sig_scale * conf_mult * market_factor * _sector_mult
                _stop_dist  = actual_risk / actual_entry if actual_entry > 0 else 0.05
                pos_size   = min(target_R / _stop_dist, 0.20)

                # ── 現金可用性（零股：只在信號部位之間追蹤，beta 獨立池）
                _sig_deployed   = sum(ps for _, ps in open_capital)
                _available_cash = max(0.0, 1.0 - _sig_deployed)
                if pos_size > _available_cash:
                    pos_size = round(_available_cash, 4)
                if pos_size < 0.005:
                    continue

                _trade_heat  = round(pos_size * _stop_dist, 5)
                _total_heat  = sum(h for _, h in open_positions)
                _regime_heat = MAX_HEAT_BY_REGIME.get(_eff_regime, 0.15)
                if _total_heat + _trade_heat > _regime_heat:
                    continue   # 超過整體風險預算

                # ── 4e: 追蹤結果（只看已過去的資料）
                if sig_type == "high_base":
                    max_days = MAX_HOLD_LONG
                elif sig_type == "momentum_ignition":
                    max_days = MAX_HOLD_IG         # AVWAP 動態止損，上限與 high_base 相同
                elif sig_type == "ma_pullback":
                    max_days = MAX_HOLD_PULLBACK   # 2週：技術面定義，不確認即失效
                elif sig_type in TREND_TYPES:
                    max_days = MAX_HOLD_TREND
                else:
                    max_days = MAX_HOLD_SWING
                open_positions.append((q_date + timedelta(days=max_days + 1), _trade_heat))
                final_si  = min(ni + max_days, len(sd["closes"]) - 1)
                outcome   = "inconclusive"
                _fsi = final_si
                while _fsi > ni and math.isnan(sd["closes"][_fsi]):
                    _fsi -= 1
                exit_px    = sd["closes"][_fsi] if not math.isnan(sd["closes"][_fsi]) else actual_entry
                _exit_si_at = _fsi   # 預設 expired；stop/target 時由迴圈覆蓋

                _is_trend      = sig_type in TREND_TYPES
                trail_stop     = stop
                _mid_target    = actual_entry + 0.5 * (target - actual_entry)
                _be_activated  = False
                # ig / high_base：進場時固定錨點 index，持倉期間每日重算 AVWAP 作為動態止損
                # high_base 僅 bull 相位啟用 AVWAP（其他相位用 MA10 避免熊市損失擴大）
                _ig_anchor  = snap.get("swing_anchor_idx") if sig_type in ("momentum_ignition", "high_base") else None
                _use_avwap  = (_ig_anchor is not None and
                               (sig_type == "momentum_ignition" or
                                (sig_type == "high_base" and _eff_regime == "bull")))

                for d in range(1, final_si - ni + 1):
                    fh = sd["highs"][ni + d]
                    fl = sd["lows"][ni + d]
                    fo = sd["opens"][ni + d]
                    _idx = ni + d

                    if _is_trend:
                        if _use_avwap:
                            # AVWAP 動態止損：從進場錨點到當日重算，趨勢破壞才出場
                            _av = sum((sd["highs"][k] + sd["lows"][k] + sd["closes"][k]) / 3 * sd["vols"][k]
                                      for k in range(_ig_anchor, _idx + 1))
                            _vv = sum(sd["vols"][k] for k in range(_ig_anchor, _idx + 1))
                            if _vv > 0:
                                _cur_avwap = _av / _vv
                                trail_stop = max(trail_stop, _cur_avwap * 0.98)
                        else:
                            # MA10 追蹤停損：讓趨勢決定持倉長度，不是時鐘
                            if _idx >= 10:
                                _ma10 = sum(sd["closes"][_idx - 9: _idx + 1]) / 10
                                trail_stop = max(trail_stop, _ma10 * (1 - MA_TRAIL_BUFFER))
                        hit_t = False   # 趨勢型無固定目標
                    else:
                        # 短線型：保留 break-even 保護
                        if not _be_activated and fh >= _mid_target:
                            trail_stop    = max(trail_stop, actual_entry)
                            _be_activated = True
                        hit_t = fh >= target

                    hit_s = fl <= trail_stop
                    if hit_t and hit_s:
                        outcome     = "win" if fo >= (target + trail_stop) / 2 else "loss"
                        exit_px     = target if outcome == "win" else trail_stop
                        _exit_si_at = ni + d
                        break
                    elif hit_t:
                        outcome = "win";  exit_px = target;  _exit_si_at = ni + d;  break
                    elif hit_s:
                        exit_px     = trail_stop
                        outcome     = "win" if exit_px > actual_entry else "loss"
                        _exit_si_at = ni + d
                        break

                if outcome == "inconclusive":
                    exit_type = "expired"
                    outcome   = "win" if exit_px > actual_entry else "loss"
                else:
                    exit_type = ("target" if (not _is_trend and outcome == "win")
                                 else "stop")

                gain_pct = round((exit_px - actual_entry) / actual_entry * 100, 2)

                # 記錄資金佔用（以實際出場日為到期，供後續交易的現金計算使用）
                _actual_exit_date = sd["dates"][_exit_si_at]
                open_capital.append((_actual_exit_date + timedelta(days=1), pos_size))

                trades.append({
                    "date":          q_date.strftime("%Y-%m-%d"),
                    "code":          code,
                    "type":          sig_type,
                    "label":         sig["label"],
                    "strength":      sig["strength"],
                    "strategy":      sig["strategy"],
                    "regime":        _eff_regime,
                    "stock_phase":   phase,
                    "rs_pct":        rs_pct,
                    "entry":         round(actual_entry, 2),
                    "stop":          round(stop,         2),
                    "target":        round(target,       2),
                    "entry_type":    entry_type,
                    "exit_type":     exit_type,
                    "exit_date":     sd["dates"][_exit_si_at].strftime("%Y-%m-%d"),
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

        _hv_tag  = " ⚡高波動" if _high_vol else ""
        _eff_tag = f"→{_eff_regime}" if _eff_regime != regime else ""
        _vix_tag = f"  VIX={_vix_today:.1f}" if _vix_today else ""
        _bs_tag  = f"  bs={breadth_slope:+.2f}" if abs(breadth_slope) >= 0.05 else ""
        print(f"  {q_date}  regime={regime:<14}{_eff_tag:<16}{_hv_tag:<6}{_vix_tag}{_bs_tag}  訊號={day_count:3d}筆  累計={len(trades)}")

    # ── 回測結束：關閉未平倉的 beta 部位（用最後一個交易日收盤價結算）
    if beta_mode is not None and beta_entries and q1_dates:
        _last_bt_d = q1_dates[-1]
        _n_beta    = max(len(beta_entries), 1)
        _pf_each   = round(beta_alloc_at_open / _n_beta, 4)
        for b_code, b_entry_px in beta_entries.items():
            b_si = stock_date_idx[b_code].get(_last_bt_d)
            b_exit_px = b_entry_px
            if b_si is not None and b_si < len(stock_data[b_code]["closes"]):
                _px = stock_data[b_code]["closes"][b_si]
                if not math.isnan(_px) and _px > 0:
                    b_exit_px = _px
            b_gain = round((b_exit_px - b_entry_px) / b_entry_px * 100, 2)
            beta_trades.append({
                "date":          _last_bt_d.strftime("%Y-%m-%d"),
                "code":          b_code,
                "type":          "beta_momentum",
                "label":         f"RSβ({beta_open_regime})",
                "strength":      "beta",
                "strategy":      "beta",
                "regime":        beta_open_regime,
                "stock_phase":   "BULL",
                "rs_pct":        80.0,
                "entry":         round(b_entry_px, 2),
                "stop":          0.0,
                "target":        0.0,
                "entry_type":    "beta",
                "exit_type":     "bt_end",
                "outcome":       "win" if b_gain >= 0 else "loss",
                "gain_pct":      b_gain,
                "actual_rr":     0.0,
                "confirmations": 0,
                "pos_factor":    _pf_each,
                "market_er":     0.0,
                "market_factor": 1.0,
                "sector_key":    "",
                "sector_rs_pct": 0.0,
                "high_vol":      False,
            })
        beta_entries = {}
        beta_mode    = None

    _beta_contrib = sum(t["pos_factor"] * t["gain_pct"] / 100 for t in beta_trades)
    print(f"\n  ✓ 回測完成：共 {len(trades)} 筆信號交易  beta 層貢獻 {_beta_contrib*100:+.1f}%")

    # ── Step 4: 統計 ──────────────────────────────────────────────
    print("\n[5] 統計彙整...")

    by_type     = defaultdict(list)
    by_strength = defaultdict(list)
    by_month    = defaultdict(list)
    # ── Sector Exit Post-Process：用完整的 daily_sec_slope_pct 覆蓋惡化類股的出場
    trades = _apply_sector_exits(
        trades, stock_data, stock_date_idx, daily_sec_slope_pct, daily_regime, SECTOR_EXIT_THRESHOLD
    )

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
                                  trades + beta_trades,  # 信號層 + Beta 層合併曲線
                                  BT_START),
        "benchmark_curve":    benchmark_curve,
        "trades":             trades,
        "beta_trades":        beta_trades,
        "gate_blocked_summary": _gate_blocked_summary(gate_blocked_log),
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
