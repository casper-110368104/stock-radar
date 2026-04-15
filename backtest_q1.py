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
from datetime import datetime, date
from collections import defaultdict
from signals import calc_signals

# ── 設定 ────────────────────────────────────────────────────────────
BT_START       = date(2025, 1, 2)    # 回測起始（台股 1/1 休市，1/2 開盤）
BT_END         = date(2026, 3, 31)   # 回測結束（含 Q1 2026）
BT_PERIOD      = "2025-Q1~2026-Q1"   # 顯示標籤
MAX_HOLD_TREND = 25
MAX_HOLD_SWING = 8
BENCHMARK_TID  = "^TWII"
OUTPUT_PATH    = "docs/backtest_q1.json"
SLIP           = 0.002    # 滑價估計 0.2%
MIN_HIST_DAYS  = 70
HEADERS        = {"User-Agent": "Mozilla/5.0 (stock-radar-backtest/1.0)"}

TREND_TYPES = {"breakout", "high_base", "trend_cont"}
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


# ── 大盤相位（截至第 i 日的 TWII）────────────────────────────────────
def _market_regime(bm_closes, i):
    if i < 60:
        return "range"
    p    = bm_closes[i]
    ma20 = sum(bm_closes[i - 19:i + 1]) / 20
    ma60 = sum(bm_closes[i - 59:i + 1]) / 60
    if p > ma20 and p > ma60:
        return "bull"
    if p > ma60 and p <= ma20:
        return "bull_pullback"
    if p < ma60 * 0.97:
        return "bear"
    return "range"


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


# ── 主程式 ────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Q1 2025 Walk-Forward Backtest")
    print(f"  期間：{BT_START} ~ {BT_END}")
    print("  原則：每日只用截至當日的已知資料，無向前看偏差")
    print("=" * 60)

    # ── Step 1: 決定母體 ─────────────────────────────────────────
    print("\n[1] 抓取股票母體（TWSE 量能前 300）...")
    print(f"  回測期間：{BT_START} ~ {BT_END}")
    universe = []
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
        universe = [c for c, _ in tmp[:300]]
        print(f"  → {len(universe)} 檔")
    except Exception as e:
        print(f"  TWSE API 失敗：{e}，使用內建清單")
        universe = [
            "2330","2317","2454","2382","2308","2303","2412","3711",
            "2881","2882","2891","2886","2884","2885","1301","1303",
            "6505","2002","2912","2207","1216","2327","3034","3037",
            "2395","2379","4938","2408","2357","2377","3008","2357",
        ]

    universe = list(dict.fromkeys(universe))

    # ── Step 2: 下載歷史資料 ─────────────────────────────────────
    # 起始日早於 Q1_START 240 個交易日（~1 年）供 RS 計算
    # 結束日晚於 Q1_END 30 個交易日供結果追蹤
    DATA_START = "2023-06-01"
    DATA_END   = "2026-06-01"

    print(f"\n[2] 下載 TWII 基準...")
    bm = yf.Ticker(BENCHMARK_TID).history(start=DATA_START, end=DATA_END)
    if bm.empty:
        print("TWII 下載失敗，中止"); sys.exit(1)
    bm_dates    = [d.date() for d in bm.index]
    bm_closes   = [float(v) for v in bm["Close"].tolist()]
    bm_date_idx = {d: i for i, d in enumerate(bm_dates)}
    q1_dates    = [d for d in bm_dates if BT_START <= d <= BT_END]
    print(f"  TWII：{len(bm_dates)} 日 | 回測交易日：{len(q1_dates)} 天")

    print(f"\n[3] 下載 {len(universe)} 檔個股資料...")
    print("    (每 20 檔暫停 3 秒避免限速，預計 5~10 分鐘)")
    stock_data = {}
    for idx, code in enumerate(universe):
        tid = code + ".TW"
        print(f"  [{idx + 1:3d}/{len(universe)}] {code}", end="  ")
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

    print(f"\n  → 有效個股：{len(stock_data)} 檔")

    # 為每支股票建立「日期 → 陣列 index」的快速對照
    stock_date_idx = {}
    for code, sd in stock_data.items():
        stock_date_idx[code] = {d: i for i, d in enumerate(sd["dates"])}

    # ── Step 3: Walk-Forward 主迴圈 ──────────────────────────────
    print(f"\n[4] Walk-Forward 逐日掃描（{len(q1_dates)} 個交易日）...")
    trades = []

    for q_date in q1_dates:
        bm_i = bm_date_idx.get(q_date)
        if bm_i is None:
            continue

        regime = _market_regime(bm_closes, bm_i)

        # ── 4a: 計算所有股票截至當日的 RS scalar，用於當日百分位排名
        rs_scalar_map = {}
        rs_cache      = {}   # code → daily_rs（當日截止），避免重複計算
        for code, sd in stock_data.items():
            si = stock_date_idx[code].get(q_date)
            if si is None or si < 10:
                continue
            # 對齊：只用到 bm_i+1 的大盤資料
            n_align    = min(si + 1, bm_i + 1)
            dr         = _daily_rs(sd["closes"][:n_align], bm_closes[:n_align])
            rs_cache[code] = dr
            _, scalar  = _rs_metrics(dr)
            if scalar is not None:
                rs_scalar_map[code] = scalar

        # ── 4b: 計算當日跨股 RS 百分位
        if len(rs_scalar_map) > 1:
            sorted_codes = sorted(rs_scalar_map, key=lambda c: rs_scalar_map[c])
            n_rs         = len(sorted_codes)
            rs_pct_map   = {c: round(i / (n_rs - 1) * 100, 1)
                            for i, c in enumerate(sorted_codes)}
        else:
            rs_pct_map = {c: 50.0 for c in rs_scalar_map}

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

            snap["m_z"]            = m_z
            snap["rs_trend_stock"] = slope
            snap["sector_rs"]      = None   # 簡化：不計板塊 RS

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
                if actual_rr < 1.2:
                    continue   # RR 不足

                # ── 4e: 追蹤結果（只看已過去的資料）
                is_trend  = sig_type in TREND_TYPES
                max_days  = MAX_HOLD_TREND if is_trend else MAX_HOLD_SWING
                final_si  = min(ni + max_days, len(sd["closes"]) - 1)
                outcome   = "inconclusive"
                exit_px   = sd["closes"][final_si]

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
                    "confirmations": sig.get("confirmations", 0),
                    "pos_factor":    sig.get("pos_factor", 0.5),
                })
                day_count += 1

        print(f"  {q_date}  regime={regime:<14}  訊號={day_count:3d}筆  累計={len(trades)}")

    print(f"\n  ✓ 回測完成：共 {len(trades)} 筆觸發交易")

    # ── Step 4: 統計 ──────────────────────────────────────────────
    print("\n[5] 統計彙整...")

    by_type     = defaultdict(list)
    by_strength = defaultdict(list)
    by_month    = defaultdict(list)
    by_quarter  = defaultdict(list)
    by_regime   = defaultdict(list)
    by_conf     = defaultdict(list)

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
        "trades":             trades,
    }

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
