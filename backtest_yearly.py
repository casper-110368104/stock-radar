#!/usr/bin/env python3
"""
backtest_yearly.py — 逐年獨立回測（樣本外驗證）

每年以全新資本起算，母體依「前一年平均成交量」選出（無向前看、無倖存者偏差）。
12/31 強制平倉所有未結持倉——任何單年若在 12/31 前未碰停損或目標，以末日收盤出場。

年份：2022（熊市）、2023（反彈）、2024（波動）、2025（多頭）
目的：測試策略在各市場環境是否有真實 edge，而非針對特定年份過擬合。

執行：python backtest_yearly.py
輸出：docs/backtest_yearly.json
"""
import json, math, sys, time, requests
import yfinance as yf
from datetime import date, timedelta, datetime
from collections import defaultdict

from signals import calc_signals
from collections import deque
from backtest_q1 import (
    _snapshot, _market_regime, _efficiency_ratio, _vol_flag,
    _daily_rs, _rs_metrics, _rs_slope, _rs_accel, _stock_phase, _classify_structure, _stats, _capital_curves,
    _vix_overlay, _breadth_divergence, _gate_blocked_summary, _apply_sector_exits,
    _compute_chips_score,
    SIGNAL_SCALE, REGIME_ACTIVE_SIGNALS, BASE_R, GAP_LIMIT, SLIP,
    MAX_HOLD_LONG, MAX_HOLD_TREND, MAX_HOLD_PULLBACK, MAX_HOLD_SWING, MAX_HOLD_IG, MA_TRAIL_BUFFER,
    MAX_HEAT_BY_REGIME, TREND_TYPES, MIN_HIST_DAYS, BENCHMARK_TID, HEADERS,
    BETA_ALLOC_MAX, BETA_ALLOC_DOMINANT, BETA_TOP_N,
    SECTOR_DOMINANCE_GAP, SECTOR_DOMINANCE_MIN, SECTOR_EXIT_THRESHOLD, SECTOR_GATE_THRESHOLD,
)

YEARS              = [2022, 2023, 2024, 2025]
OUTPUT_PATH        = "docs/backtest_yearly.json"
DATA_START         = "2020-01-01"   # 足夠計算 MA60 / RS240
DATA_END           = "2026-06-01"
UNIVERSE_DAILY_N = 300   # 每日動態候選池大小（依當日前60日均量排名）


def main():
    print("=" * 60)
    print("  逐年獨立回測（樣本外驗證）")
    print(f"  測試年份：{YEARS}")
    print("  設計：每年 100 萬重算、12/31 強制平倉、無任何向前看")
    print("=" * 60)

    # ── 板塊對應（sector_map.json 為主，stocks.json 補充）──────────────
    sector_map   = {}
    sector_codes = defaultdict(list)
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
            print(f"  板塊載入失敗（{_e}）")

    # ── 載入歷史籌碼資料 ───────────────────────────────────────────
    from pathlib import Path
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

    # ── 下載 TWII + 0050（一次，全區間共用）──────────────────────
    print(f"\n[1] 下載 TWII + 0050 + VIX...")
    bm = yf.Ticker(BENCHMARK_TID).history(start=DATA_START, end=DATA_END)
    if bm.empty:
        print("TWII 下載失敗，中止"); sys.exit(1)
    bm_dates    = [d.date() for d in bm.index]
    bm_closes   = [float(v) for v in bm["Close"].tolist()]
    bm_date_idx = {d: i for i, d in enumerate(bm_dates)}
    print(f"  TWII：{len(bm_dates)} 日")

    # 0050 基準（正規化至各年 1/1 = 1.0）
    etf0050_dict = {}
    try:
        _e = yf.Ticker("0050.TW").history(start=DATA_START, end=DATA_END)
        if not _e.empty:
            etf0050_dict = {d.date(): float(c)
                            for d, c in zip(_e.index, _e["Close"].tolist())}
            print(f"  0050：{len(etf0050_dict)} 日")
    except Exception as _be:
        print(f"  0050 下載失敗（{_be}），benchmark 跳過")

    # VIX（各年共用）
    vix_dict = {}
    try:
        _vr = yf.Ticker("^VIX").history(start=DATA_START, end=DATA_END)
        if not _vr.empty:
            vix_dict = {d.date(): float(v)
                        for d, v in zip(_vr.index, _vr["Close"].tolist())}
            print(f"  VIX：{len(vix_dict)} 日")
    except Exception as _ve:
        print(f"  VIX 下載失敗（{_ve}），overlay 跳過")

    # ── 下載個股（一次，各年共用）──────────────────────────────────
    print(f"\n[2] 抓取所有 TWSE 上市股票代號...")
    candidates = []
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json",
            headers=HEADERS, timeout=15)
        _d = r.json()
        fields  = _d.get("fields", [])
        rows    = _d.get("data", [])
        i_code  = fields.index("證券代號") if "證券代號" in fields else 0
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
        candidates = tmp
    except Exception as e:
        print(f"  TWSE API 失敗（{e}），使用內建清單")
        candidates = [
            "2330","2317","2454","2382","2308","2303","2412","3711",
            "2881","2882","2891","2886","2884","2885","1301","1303",
            "6505","2002","2912","2207","1216","2327","3034","3037",
        ]
    candidates = list(dict.fromkeys(candidates))

    print(f"\n[3] 下載 {len(candidates)} 檔個股資料（DATA_START={DATA_START}）...")
    print("    （每 20 檔暫停 3 秒，預計 15~25 分鐘）")
    stock_data = {}
    for idx, code in enumerate(candidates):
        print(f"  [{idx+1:3d}/{len(candidates)}] {code}", end="  ")
        try:
            h = yf.Ticker(code + ".TW").history(start=DATA_START, end=DATA_END)
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
    print(f"  → 下載完成：{len(stock_data)} 檔（每日動態依前60日均量選 Top {UNIVERSE_DAILY_N}）")

    # 建立全域「日期 → 陣列 index」對照（各年共用）
    stock_date_idx = {
        code: {d: i for i, d in enumerate(sd["dates"])}
        for code, sd in stock_data.items()
    }

    # ── 逐年回測 ────────────────────────────────────────────────────
    yearly_results = {}

    for year in YEARS:
        print(f"\n{'='*60}")
        print(f"  {year} 年回測")
        print(f"{'='*60}")

        bt_start = date(year, 1, 2)
        bt_end   = date(year, 12, 31)

        year_dates = [d for d in bm_dates if bt_start <= d <= bt_end]
        if not year_dates:
            print(f"  {year} 無交易日，跳過"); continue
        last_year_date = max(year_dates)

        # Walk-Forward 主迴圈
        trades            = []
        open_positions    = []
        breadth_history   = deque(maxlen=15)
        vix_history       = deque(maxlen=25)
        sector_rs_history   = defaultdict(lambda: deque(maxlen=20))
        gate_blocked_log    = []
        daily_sec_slope_pct = {}   # {date: {sector: slope_pct}}
        daily_regime        = {}   # {date: regime}

        # ── RS Beta Layer 狀態（每年重置）
        beta_trades        = []
        beta_mode          = None
        beta_entries       = {}
        beta_entry_date    = None
        beta_alloc_at_open = 0.0
        beta_open_regime   = None

        open_capital        = []

        for q_date in year_dates:
            bm_i = bm_date_idx.get(q_date)
            if bm_i is None:
                continue

            # 動態宇宙 + RS scalar + 市場廣度（單次遍歷）
            rs_scalar_map = {}
            rs_cache      = {}
            _day_vols     = []
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
                if si >= 20:
                    _n = min(si, 59)
                    _vslice = sd["vols"][si - _n: si + 1]
                    _valid  = [v for v in _vslice if not math.isnan(v) and v > 0]
                    if _valid:
                        _day_vols.append((code, sum(_valid) / len(_valid)))

            _day_vols.sort(key=lambda x: x[1], reverse=True)
            _today_universe = {code for code, _ in _day_vols[:UNIVERSE_DAILY_N]}

            # 廣度 — 只計算動態宇宙內的股票
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

            # 快速廣度：5日正報酬（動態宇宙內）
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

            # 廣度動能快層
            _b_hist = list(breadth_history)
            _b_ref  = _b_hist[-10] if len(_b_hist) >= 10 else (_b_hist[0] if _b_hist else breadth_pct)
            breadth_slope = round(breadth_pct - _b_ref, 4)

            regime = _market_regime(bm_closes, bm_i, breadth_pct, breadth_slope, fast_breadth_pct)

            # market_factor（廣度 × 動能 × ER × vol）
            twii_mom_20 = (bm_closes[bm_i] / bm_closes[bm_i - 20] - 1) if bm_i >= 20 else 0.0
            _brf        = max(0.3, min(1.5, breadth_pct / 0.5))
            _mmf        = max(0.3, min(1.5, 1.0 + twii_mom_20))
            _er         = _efficiency_ratio(bm_closes, bm_i, 20)
            _er_scale   = max(0.3, min(_er / 0.30, 1.2))
            _high_vol   = _vol_flag(bm_closes, bm_i)
            _vol_mult   = 0.5 if _high_vol else 1.0
            market_factor = round(max(0.3, min(1.5, _brf * _mmf * _er_scale * _vol_mult)), 3)

            # VIX overlay + 廣度背離
            breadth_history.append(breadth_pct)
            _vix_today = vix_dict.get(q_date)
            if _vix_today is None:
                for _doff in (1, 2, 3):
                    _vix_today = vix_dict.get(q_date - timedelta(days=_doff))
                    if _vix_today is not None:
                        break
            vix_history.append(_vix_today)
            _vix_mult, _reversal_probe = _vix_overlay(_vix_today, list(vix_history)[:-1])
            market_factor = round(max(0.0, market_factor * _vix_mult), 3)
            _bd = (regime == "bear"
                   and _breadth_divergence(bm_closes, bm_i, list(breadth_history)))
            _eff_regime = ("reversal_probe"
                           if regime == "bear" and (_reversal_probe or _bd)
                           else regime)

            open_positions = [(ed, h)  for ed, h  in open_positions if ed > q_date]
            open_capital   = [(ed, ps) for ed, ps in open_capital   if ed > q_date]

            # RS 百分位
            if len(rs_scalar_map) > 1:
                _sc = sorted(rs_scalar_map, key=lambda c: rs_scalar_map[c])
                _nr = len(_sc)
                rs_pct_map = {c: round(i / (_nr - 1) * 100, 1) for i, c in enumerate(_sc)}
            else:
                rs_pct_map = {c: 50.0 for c in rs_scalar_map}

            # 類股 RS 百分位
            _sec_sum = defaultdict(float)
            _sec_cnt = defaultdict(int)
            for _c, _sv in rs_scalar_map.items():
                _sk = sector_map.get(_c, "")
                if _sk:
                    _sec_sum[_sk] += _sv
                    _sec_cnt[_sk] += 1
            _sec_avg = {sk: _sec_sum[sk] / _sec_cnt[sk] for sk in _sec_sum}
            if len(_sec_avg) > 1:
                _ss = sorted(_sec_avg, key=lambda s: _sec_avg[s])
                _sn = len(_ss)
                _sec_pct = {s: round(i / (_sn - 1) * 100, 1) for i, s in enumerate(_ss)}
            else:
                _sec_pct = {s: 50.0 for s in _sec_avg}

            # 類股動能（sector RS slope）：10日變化速率百分位
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
            _sec_combined_pct = {
                sk: round(0.5 * _sec_pct.get(sk, 50.0) + 0.5 * _sec_slope_pct.get(sk, 50.0), 1)
                for sk in _sec_avg
            }

            # 記錄每日類股斜率百分位與相位
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

            if beta_mode is not None and not _in_bull:
                _n_beta    = max(len(beta_entries), 1)
                _pf_each   = round(beta_alloc_at_open / _n_beta, 4)
                _exit_type = "regime_change"
                for b_code, b_entry_px in beta_entries.items():
                    b_si = stock_date_idx.get(b_code, {}).get(q_date)
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

            if beta_mode is None and _in_bull:
                top_codes   = sorted(rs_pct_map, key=lambda c: rs_pct_map[c], reverse=True)[:BETA_TOP_N]
                _beta_alloc = round(market_factor * BETA_ALLOC_MAX, 3)
                new_beta = {}
                for b_code in top_codes:
                    b_si = stock_date_idx.get(b_code, {}).get(q_date)
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

            day_count    = 0
            daily_hb_cnt = 0
            daily_ig_cnt = 0
            daily_tc_cnt = 0

            for code, sd in sorted(
                    ((c, stock_data[c]) for c in _today_universe if c in stock_data),
                    key=lambda x: rs_pct_map.get(x[0], 0),
                    reverse=True):
                si = stock_date_idx[code].get(q_date)
                if si is None or si < 62:
                    continue
                if sd["dates"][si] != q_date:
                    continue
                if si + 1 >= len(sd["closes"]):
                    continue

                snap = _snapshot(sd["closes"], sd["highs"], sd["lows"],
                                 sd["vols"], sd["opens"], si)
                if snap is None:
                    continue

                rs_pct      = rs_pct_map.get(code, 50.0)
                dr          = rs_cache.get(code, [])
                m_z, _      = _rs_metrics(dr)
                slope       = _rs_slope(dr)
                accel       = _rs_accel(dr)
                stock_roc5  = (sd["closes"][si] / sd["closes"][si - 5] - 1) if si >= 5 else 0.0
                _code_sk           = sector_map.get(code, "")
                _code_sec_pct      = _sec_pct.get(_code_sk, 50.0)
                _code_sec_combined = _sec_combined_pct.get(_code_sk, 50.0)


                snap["m_z"]             = m_z
                snap["rs_trend_stock"]  = slope
                snap["rs_accel"]        = accel
                snap["sector_rs"]       = _code_sec_pct
                snap["sector_combined"] = _code_sec_combined
                snap["rs_pct_val"]      = rs_pct

                phase = _stock_phase(rs_pct, m_z, snap)

                _sk_slope = _sec_slope_pct.get(_code_sk, 50.0)
                if _sk_slope >= 60:
                    _sector_phase = "主升段"
                elif _sk_slope >= 40:
                    _sector_phase = "主升回檔"
                elif _sk_slope <= 25:
                    _sector_phase = "空頭"
                else:
                    _sector_phase = ""

                structure = _classify_structure(snap, phase, _sector_phase)

                # 1. 量能分數（與 fetch_stocks.py 一致）
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

                sigs  = calc_signals(snap, {"chips_score_val": _chips_score_val}, rs_pct,
                                     stock_phase=phase,
                                     market_regime=regime,
                                     composite_score=_tech_score,
                                     structure=structure,
                                     sector_phase=_sector_phase)
                if not sigs:
                    continue

                ni        = si + 1
                nxt_open  = sd["opens"][ni]
                nxt_high  = sd["highs"][ni]

                # 年末強制平倉：持倉不跨年
                _last_code_si = max(
                    (i for i, d in enumerate(sd["dates"]) if d <= last_year_date),
                    default=ni
                )

                for sig in sigs:
                    trigger  = sig.get("trigger_price", sig["entry"])
                    stop     = sig["stop_loss"]
                    target   = sig["target"]
                    sig_type = sig["type"]

                    if nxt_open > trigger:
                        gap_pct = (nxt_open - trigger) / trigger
                        if gap_pct > GAP_LIMIT.get(sig_type, 0.02):
                            continue
                        actual_entry = round(nxt_open * (1 + SLIP), 2)
                        entry_type   = "gap_up"
                    elif nxt_high >= trigger:
                        actual_entry = round(trigger * (1 + SLIP), 2)
                        entry_type   = "intraday"
                    else:
                        continue

                    if actual_entry > 0 and (actual_entry - stop) / actual_entry > 0.10:
                        stop = round(actual_entry * 0.90, 2)
                    actual_risk = actual_entry - stop
                    if actual_risk <= 0:
                        continue
                    actual_rr = round((target - actual_entry) / actual_risk, 2)
                    if actual_rr < 1.5:
                        continue
                    if sig_type == "retest":
                        continue
                    if sig_type not in REGIME_ACTIVE_SIGNALS.get(_eff_regime, set()):
                        continue

                    # RS 加速篩選：震盪/回檔相位只取 RS 持續上升的個股
                    if _eff_regime in ("range", "bull_pullback") and slope <= 0:
                        continue

                    # high_base：要求 RS 動能正在加速（二階導數 > 0），過濾峰值後退燒的訊號
                    if sig_type == "high_base" and (accel is None or accel <= 0):
                        continue

                    # high_base：個股5日絕對動能確認（股票本身需在上漲，排除廣度好但個股已轉弱）
                    if sig_type == "high_base" and stock_roc5 <= 0:
                        continue

                    # 每日信號密度上限：high_base ≤ 3、trend_cont ≤ 4、momentum_ignition ≤ 2
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

                    confs         = sig.get("confirmations", 0)
                    conf_mult     = 1.2 if confs >= 5 else (1.1 if confs >= 4 else 1.0)
                    sig_scale     = SIGNAL_SCALE.get(sig_type, 1.0)
                    _sector_mult  = round(0.7 + 0.006 * _code_sec_combined, 3)
                    target_R      = BASE_R * sig_scale * conf_mult * market_factor * _sector_mult
                    _stop_dist    = actual_risk / actual_entry if actual_entry > 0 else 0.05
                    pos_size      = min(target_R / _stop_dist, 0.20)

                    # 現金可用性（零股：按比例縮小；beta 獨立資金池，不佔信號現金）
                    _sig_deployed   = sum(ps for _, ps in open_capital)
                    _available_cash = max(0.0, 1.0 - _sig_deployed)
                    if pos_size > _available_cash:
                        pos_size = round(_available_cash, 4)
                    if pos_size < 0.005:
                        continue

                    _trade_heat   = round(pos_size * _stop_dist, 5)
                    _total_heat   = sum(h for _, h in open_positions)
                    _regime_heat  = MAX_HEAT_BY_REGIME.get(_eff_regime, 0.15)
                    if _total_heat + _trade_heat > _regime_heat:
                        continue

                    if sig_type == "high_base":
                        max_days = MAX_HOLD_LONG
                    elif sig_type == "momentum_ignition":
                        max_days = MAX_HOLD_IG
                    elif sig_type == "ma_pullback":
                        max_days = MAX_HOLD_PULLBACK
                    elif sig_type in TREND_TYPES:
                        max_days = MAX_HOLD_TREND
                    else:
                        max_days = MAX_HOLD_SWING

                    # 年末上限：不跨年持倉
                    final_si = min(ni + max_days, _last_code_si, len(sd["closes"]) - 1)
                    open_positions.append((q_date + timedelta(days=max_days + 1), _trade_heat))

                    outcome     = "inconclusive"
                    _fsi        = final_si
                    while _fsi > ni and math.isnan(sd["closes"][_fsi]):
                        _fsi -= 1
                    exit_px     = sd["closes"][_fsi] if not math.isnan(sd["closes"][_fsi]) else actual_entry
                    _exit_si_at = _fsi

                    _is_trend     = sig_type in TREND_TYPES
                    trail_stop    = stop
                    _mid_target   = actual_entry + 0.5 * (target - actual_entry)
                    _be_activated = False
                    _ig_anchor  = snap.get("swing_anchor_idx") if sig_type in ("momentum_ignition", "high_base") else None
                    _use_avwap  = (_ig_anchor is not None and
                                   (sig_type == "momentum_ignition" or
                                    (sig_type == "high_base" and _eff_regime == "bull")))

                    for d_off in range(1, final_si - ni + 1):
                        fh   = sd["highs"][ni + d_off]
                        fl   = sd["lows"][ni + d_off]
                        fo   = sd["opens"][ni + d_off]
                        _idx = ni + d_off

                        if _is_trend:
                            if _use_avwap:
                                _av = sum((sd["highs"][k] + sd["lows"][k] + sd["closes"][k]) / 3 * sd["vols"][k]
                                          for k in range(_ig_anchor, _idx + 1))
                                _vv = sum(sd["vols"][k] for k in range(_ig_anchor, _idx + 1))
                                if _vv > 0:
                                    _cur_avwap = _av / _vv
                                    trail_stop = max(trail_stop, _cur_avwap * 0.98)
                            else:
                                if _idx >= 10:
                                    _ma10 = sum(sd["closes"][_idx - 9: _idx + 1]) / 10
                                    trail_stop = max(trail_stop, _ma10 * (1 - MA_TRAIL_BUFFER))
                            hit_t = False
                        else:
                            if not _be_activated and fh >= _mid_target:
                                trail_stop    = max(trail_stop, actual_entry)
                                _be_activated = True
                            hit_t = fh >= target

                        hit_s = fl <= trail_stop
                        if hit_t and hit_s:
                            outcome     = "win" if fo >= (target + trail_stop) / 2 else "loss"
                            exit_px     = target if outcome == "win" else trail_stop
                            _exit_si_at = ni + d_off
                            break
                        elif hit_t:
                            outcome = "win";  exit_px = target;  _exit_si_at = ni + d_off;  break
                        elif hit_s:
                            exit_px     = trail_stop
                            outcome     = "win" if exit_px > actual_entry else "loss"
                            _exit_si_at = ni + d_off
                            break

                    if outcome == "inconclusive":
                        exit_type = "expired"
                        outcome   = "win" if exit_px > actual_entry else "loss"
                    else:
                        exit_type = ("target" if (not _is_trend and outcome == "win")
                                     else "stop")

                    gain_pct = round((exit_px - actual_entry) / actual_entry * 100, 2)

                    _actual_exit_date = sd["dates"][_exit_si_at]
                    open_capital.append((_actual_exit_date + timedelta(days=1), pos_size))

                    trades.append({
                        "date":           q_date.strftime("%Y-%m-%d"),
                        "code":           code,
                        "type":           sig_type,
                        "label":          sig["label"],
                        "strength":       sig["strength"],
                        "strategy":       sig["strategy"],
                        "regime":         _eff_regime,
                        "stock_phase":    phase,
                        "rs_pct":         rs_pct,
                        "entry":          round(actual_entry, 2),
                        "stop":           round(stop,         2),
                        "target":         round(target,       2),
                        "entry_type":     entry_type,
                        "exit_type":      exit_type,
                        "exit_date":      sd["dates"][_exit_si_at].strftime("%Y-%m-%d"),
                        "outcome":        outcome,
                        "gain_pct":       gain_pct,
                        "actual_rr":      actual_rr,
                        "confirmations":  confs,
                        "pos_factor":     round(pos_size, 4),
                        "market_er":      round(_er, 3),
                        "market_factor":  market_factor,
                        "sector_key":     _code_sk,
                        "sector_rs_pct":  round(_code_sec_pct, 1),
                        "high_vol":       _high_vol,
                    })
                    day_count += 1

            _hv      = " ⚡" if _high_vol else ""
            _eff_tag = f"→{_eff_regime}" if _eff_regime != regime else ""
            print(f"  {q_date}  {regime:<14}{_eff_tag:<16}{_hv}  訊號={day_count:2d}筆  累計={len(trades)}")

        # ── 年末強制關閉 beta 部位（不跨年持倉）
        if beta_mode is not None and beta_entries and year_dates:
            _yr_last_d = year_dates[-1]
            _n_beta    = max(len(beta_entries), 1)
            _pf_each   = round(beta_alloc_at_open / _n_beta, 4)
            for b_code, b_entry_px in beta_entries.items():
                b_si = stock_date_idx.get(b_code, {}).get(_yr_last_d)
                b_exit_px = b_entry_px
                if b_si is not None and b_si < len(stock_data[b_code]["closes"]):
                    _px = stock_data[b_code]["closes"][b_si]
                    if not math.isnan(_px) and _px > 0:
                        b_exit_px = _px
                b_gain = round((b_exit_px - b_entry_px) / b_entry_px * 100, 2)
                beta_trades.append({
                    "date":          _yr_last_d.strftime("%Y-%m-%d"),
                    "code":          b_code,
                    "type":          "beta_momentum",
                    "label":         "RSβ-bull",
                    "strength":      "beta",
                    "strategy":      "beta",
                    "regime":        beta_open_regime,
                    "stock_phase":   "BULL",
                    "rs_pct":        80.0,
                    "entry":         round(b_entry_px, 2),
                    "stop":          0.0,
                    "target":        0.0,
                    "entry_type":    "beta",
                    "exit_type":     "year_end",
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
        print(f"\n  ✓ {year} 走步回測完成：{len(trades)} 筆信號  beta 層貢獻 {_beta_contrib*100:+.1f}%")

        # Sector Exit Post-Process
        trades = _apply_sector_exits(
            trades, stock_data, stock_date_idx, daily_sec_slope_pct, daily_regime, SECTOR_EXIT_THRESHOLD
        )

        # 統計（信號交易）
        overall   = _stats(trades)
        by_type   = defaultdict(list)
        by_regime = defaultdict(list)
        by_month  = defaultdict(list)
        for t in trades:
            by_type[t["type"]].append(t)
            by_regime[t["regime"]].append(t)
            by_month[t["date"][:7]].append(t)

        all_trades = sorted(trades + beta_trades, key=lambda t: t["date"])
        curves     = _capital_curves(all_trades, bt_start)

        # 0050 本年比較基準（正規化：year 第一個交易日 = 1.0）
        _bench = []
        if etf0050_dict:
            _yr_base_d = next((d for d in sorted(etf0050_dict) if d >= bt_start), None)
            _yr_base_px = etf0050_dict[_yr_base_d] if _yr_base_d else None
            if _yr_base_px:
                _bench = [
                    {"date": d.strftime("%Y-%m-%d"), "equity": round(etf0050_dict[d] / _yr_base_px, 4)}
                    for d in sorted(etf0050_dict) if bt_start <= d <= bt_end
                ]

        _sig_contrib = sum(t.get("pos_factor", 0) * t.get("gain_pct", 0) / 100 for t in trades)
        print(f"\n  === {year} 結果 ===")
        print(f"  筆數={overall['count']}  WR={overall['win_rate']}%  EV={str(overall['expectancy'])+'%'}")
        print(f"  信號層: {_sig_contrib*100:+.1f}%  Beta層: {_beta_contrib*100:+.1f}%")
        print(f"  固定: {curves['fixed']['total_return_pct']:+.1f}%  MaxDD={curves['fixed']['max_drawdown_pct']:.1f}%")
        print(f"  複利: {curves['compound']['total_return_pct']:+.1f}%  MaxDD={curves['compound']['max_drawdown_pct']:.1f}%")

        yearly_results[str(year)] = {
            "year":           year,
            "trading_days":   len(year_dates),
            "universe_size":  UNIVERSE_DAILY_N,
            "overall":        overall,
            "by_type":        {k: _stats(v) for k, v in by_type.items()},
            "by_regime":      {k: _stats(v) for k, v in by_regime.items()},
            "by_month":       {k: _stats(v) for k, v in sorted(by_month.items())},
            "capital_curves": curves,
            "benchmark_curve": _bench,
            "trades":         trades,
            "beta_trades":    beta_trades,
            "gate_blocked_summary": _gate_blocked_summary(gate_blocked_log),
        }

    # ── 最終輸出 ─────────────────────────────────────────────────────
    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "years":        YEARS,
        "yearly":       yearly_results,
        "summary": {
            str(y): {
                "return_fixed":    yearly_results[str(y)]["capital_curves"]["fixed"]["total_return_pct"],
                "maxdd_fixed":     yearly_results[str(y)]["capital_curves"]["fixed"]["max_drawdown_pct"],
                "return_compound": yearly_results[str(y)]["capital_curves"]["compound"]["total_return_pct"],
                "maxdd_compound":  yearly_results[str(y)]["capital_curves"]["compound"]["max_drawdown_pct"],
                "win_rate":        yearly_results[str(y)]["overall"]["win_rate"],
                "expectancy":      yearly_results[str(y)]["overall"]["expectancy"],
                "count":           yearly_results[str(y)]["overall"]["count"],
            }
            for y in YEARS if str(y) in yearly_results
        }
    }

    def _sanitize(obj):
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        return obj

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(_sanitize(result), f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print("  四年回測摘要")
    print(f"{'='*60}")
    print(f"  {'年份':>6}  {'筆數':>5}  {'WR':>7}  {'EV':>8}  {'固定報酬':>9}  {'MaxDD':>7}  備注")
    notes = {2022: "熊市（真正樣本外）", 2023: "反彈（真正樣本外）",
             2024: "波動+閃崩",         2025: "多頭延伸"}
    for y in YEARS:
        if str(y) not in result["summary"]:
            continue
        s = result["summary"][str(y)]
        print(f"  {y:>6}  {s['count']:>5}  {str(s['win_rate'])+'%':>7}  "
              f"{(str(s['expectancy'])+'%') if s['expectancy'] is not None else 'N/A':>8}  "
              f"{s['return_fixed']:>+9.1f}%  {s['maxdd_fixed']:>6.1f}%  {notes.get(y,'')}")
    print(f"\n  ✓ 結果已寫入 {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
