"""
signals.py — 共用訊號偵測模組

fetch_stocks.py 與 fetch_expansion.py 共同 import，確保訊號邏輯完全一致。
任何訊號條件修改只需改這一個檔案。
"""

_RR_MAP = {
    "BULL":          3.0,
    "BULL_PULLBACK": 2.0,
    "RANGE":         1.5,
    "BEAR_WEAK":     1.5,
    "BEAR_STRONG":   1.0,
}

# 大盤相位目標乘數：bull 市場放寬目標，bear 市場收緊
_REGIME_TARGET_MULT = {
    "bull":          1.2,
    "bull_pullback": 1.0,
    "range":         0.85,
    "bear":          0.7,
}

# ATR 止損乘數：趨勢訊號給更大空間，短線訊號更緊；熊市全面縮緊
_ATR_MULT = {
    "trend": {"bull": 2.5, "bull_pullback": 2.0, "range": 2.0, "bear": 1.5},
    "swing": {"bull": 1.5, "bull_pullback": 1.2, "range": 1.2, "bear": 1.0},
}

_ALLOWED_SIGNALS = {
    "BULL":          {"breakout", "false_breakdown", "ma_pullback", "high_base",
                      "retest", "trend_cont"},
    # ma60_support 只在個股回檔期才有意義（BULL 時價格遠高於 MA60，觸碰 MA60 = 趨勢轉弱）
    "BULL_PULLBACK": {"ma_pullback", "retest", "ma60_support"},
    "RANGE":         {"ma_pullback", "retest"},
    "BEAR_WEAK":     {"false_breakdown", "retest"},
    "BEAR_STRONG":   {"false_breakdown"},
}


def calc_signals(yahoo, chips=None, rs_pct=50, stock_phase="RANGE",
                 market_regime="range", composite_score=0, structure="",
                 sector_phase=""):
    """
    偵測技術面買點訊號，回傳 list of dict。
    每個訊號欄位：
      type, label, strength, strategy, entry, stop_loss, target,
      risk, rr, reason, trigger_price, atr_stop, timeframe,
      confirmations, confirmation_flags

    參數：
      chips:           chips dict（含 chips_score_val），None 時視為無籌碼資料
      market_regime:   'bull'|'bull_pullback'|'range'|'bear'，預設 'range'
      composite_score: 綜合評分；> 0 且 < 45 時不產生任何訊號（弱股保護）
      structure:       個股結構標籤（趨勢延伸訊號需要此欄位）
    """
    signals = []

    # 弱股保護門檻
    if composite_score > 0 and composite_score < 45:
        return []

    price      = yahoo.get("price")      or 0
    high20     = yahoo.get("high20")
    low20      = yahoo.get("low20")
    prev_low20 = yahoo.get("prev_low20")
    prev_close = yahoo.get("prev_close")
    ma5        = yahoo.get("ma5")
    ma10       = yahoo.get("ma10")
    ma20       = yahoo.get("ma20")
    ma60       = yahoo.get("ma60")
    vol_day     = yahoo.get("vol_day_ratio") or 1.0
    avwap_swing = yahoo.get("avwap_swing")
    avwap_vol   = yahoo.get("avwap_vol")
    avwap_short = yahoo.get("avwap_short")
    m_z_val      = yahoo.get("m_z")
    # 同時相容兩種 key（fetch_stocks 用 rs_trend_stock，fetch_expansion 用 rs_trend）
    rs_trend_val = yahoo.get("rs_trend_stock") or yahoo.get("rs_trend")
    sector_rs    = yahoo.get("sector_rs")

    if not price:
        return []

    # AVWAP 狀態標記
    _trend_ok = avwap_swing is None or price >= avwap_swing
    _mm_ok    = avwap_vol   is None or price >= avwap_vol
    _short_ok = avwap_short is None or price >= avwap_short

    # BULL 型態動能額外條件：M>1.2 且 RS 斜率向上 且 產業RS≥0（中性產業允許）
    _bull_momentum = (stock_phase != "BULL") or (
        m_z_val is not None and m_z_val > 1.2 and
        rs_trend_val is not None and rs_trend_val > 0 and
        (sector_rs is None or sector_rs >= 0)
    )

    # 大盤相位過濾
    # 注意：'range' 市場採用個股型態過濾（_base_allowed），不額外限縮訊號種類，
    # 這樣 expansion（無大盤相位資料時預設 range）仍能看到完整訊號。
    _market_bear  = (market_regime == "bear")
    _base_allowed = _ALLOWED_SIGNALS.get(stock_phase, _ALLOWED_SIGNALS["RANGE"])
    _MARKET_ALLOWED = {
        "bull":          _base_allowed,
        "bull_pullback": {"ma_pullback", "retest", "false_breakdown", "ma60_support"},
        "range":         _base_allowed,
        "bear":          {"false_breakdown"},
    }.get(market_regime, _base_allowed)

    # 訊號確認旗標（最高 6 項；chips/market 無資料時對應項目 ok=False）
    _chips_score = (chips or {}).get("chips_score_val", 0) or 0
    _conf_flags = [
        {"lbl": "籌碼偏多",   "sub": "主力籌碼分>60",     "ok": _chips_score > 60},
        {"lbl": "RS強勢",     "sub": "RS百分位≥70",        "ok": rs_pct >= 70},
        {"lbl": "量能擴張",   "sub": "日量比>1.3×",        "ok": (yahoo.get("vol_day_ratio") or 1) > 1.3},
        {"lbl": "個股多頭",   "sub": "個股相位=BULL",       "ok": stock_phase == "BULL"},
        {"lbl": "大盤多頭",   "sub": "大盤相位=多頭",       "ok": market_regime == "bull"},
        {"lbl": "均量線對齊", "sub": "三條AVWAP均低於現價", "ok": bool(
            avwap_swing and avwap_vol and avwap_short
            and price >= avwap_swing and price >= avwap_vol and price >= avwap_short)},
    ]
    _confirmations = sum(f["ok"] for f in _conf_flags)

    # 板塊空頭期間：壓制所有多頭訊號，只保留逆勢假跌破
    _sector_bear = (sector_phase == "空頭")

    def _sig(type_, label, strength, entry, stop, reason):
        # 大盤相位篩選（優先）
        if type_ not in _MARKET_ALLOWED:
            return None
        # 個股型態篩選
        if type_ not in _ALLOWED_SIGNALS.get(stock_phase, _ALLOWED_SIGNALS["RANGE"]):
            return None
        # 板塊空頭過濾：只允許 false_breakdown，其餘訊號全部壓制
        if _sector_bear and type_ != "false_breakdown":
            return None

        # ── 策略類型（必須在 ATR 計算前確定，修正原本 UnboundLocalError）──
        _TREND_TYPES = {"breakout", "high_base", "trend_cont"}
        _SWING_TYPES = {"false_breakdown", "ma60_support"}
        if type_ in _TREND_TYPES:
            _strategy = "trend"
        elif type_ in _SWING_TYPES:
            _strategy = "swing"
        else:  # ma_pullback, retest：強度決定策略
            _strategy = "swing" if strength == "weak" else "trend"

        _strength = strength
        _reason   = reason

        # ── AVWAP 破位處理（#4 更決策化）──
        # 趨勢型訊號（breakout/trend_cont）：直接封鎖，不產生訊號
        # 其他訊號：降一級強度並附加警告
        if not _trend_ok:
            if type_ in ("breakout", "trend_cont"):
                return None
            _strength = {"strong": "medium", "medium": "weak"}.get(_strength, _strength)
            _reason   = reason + "；⚠️趨勢破 AVWAP"

        # 大盤熊市：訊號強度上限降為 weak
        if _market_bear:
            _strength = "weak"

        # 主力成本（avwap_vol）備註
        if _mm_ok and avwap_vol:
            _reason = _reason + "；主力未跑✓"

        # ── 確認旗標加成（#2）：≥5 項確認 → 強度升一級 ──
        if _confirmations >= 5 and not _market_bear:
            _strength = {"weak": "medium", "medium": "strong"}.get(_strength, _strength)

        risk = round(entry - stop, 2) if stop else 0
        if risk <= 0:
            return None

        # 動態 RR（AVWAP 位置微調 × 大盤相位乘數）
        rr = _RR_MAP.get(stock_phase, 2.0)
        if avwap_swing and price >= avwap_swing:
            rr *= 1.2
        elif avwap_short and price < avwap_short:
            rr *= 0.7
        rr = round(rr * _REGIME_TARGET_MULT.get(market_regime, 1.0), 2)

        target = round(entry + risk * rr, 2)

        # ── ATR 動態停損（#3）──
        # 取 ATR停損 與 固定停損 中較寬者（避免正常波動震出；ATR=None 時沿用固定停損）
        atr = yahoo.get("atr_14")
        _atr_mult = _ATR_MULT[_strategy].get(market_regime, 2.0)
        atr_stop = round(entry - _atr_mult * atr, 2) if atr is not None else None
        if atr_stop is not None and atr_stop < stop:
            # ATR 停損更寬，重算 risk/target
            stop   = atr_stop
            risk   = round(entry - stop, 2)
            if risk <= 0:
                return None
            target = round(entry + risk * rr, 2)

        # 出場模式：趨勢訊號用追蹤停損（無固定目標），短線訊號用固定目標
        _TRAILING_TYPES_SET = {"breakout", "high_base", "trend_cont"}
        exit_style = "trailing" if type_ in _TRAILING_TYPES_SET else "fixed"

        # 觸發進場價：今日高 × (1 + buffer)
        _TRIGGER_BUFFER = {
            "breakout":        0.002,
            "high_base":       0.003,
            "false_breakdown": 0.003,
            "ma_pullback":     0.005,
            "retest":          0.005,
            "ma60_support":    0.005,
            "trend_cont":      0.003,
        }
        _today_high = yahoo.get("high") or entry
        trigger_price = round(_today_high * (1 + _TRIGGER_BUFFER.get(type_, 0.003)), 2)
        _TF = {"retest": "short", "false_breakdown": "short", "ma60_support": "long"}

        # 依風險比例建議倉位：目標每筆交易風險 = 1.5% 資產
        # risk_pct = 停損幅度（%），pos_factor = 建議倉位係數（0.3~1.0）
        _risk_pct = round(risk / entry * 100, 2) if entry > 0 else 3.0
        _pos_factor = round(min(1.0, max(0.3, 1.5 / _risk_pct)), 2) if _risk_pct > 0 else 0.5

        return {
            "type":               type_,
            "label":              label,
            "strength":           _strength,
            "strategy":           _strategy,
            "entry":              round(entry, 2),
            "trigger_price":      trigger_price,
            "stop_loss":          round(stop,  2),
            "atr_stop":           atr_stop,
            "target":             target,
            "risk":               risk,
            "rr":                 rr,
            "risk_pct":           _risk_pct,
            "pos_factor":         _pos_factor,
            "reason":             _reason,
            "timeframe":          _TF.get(type_, "medium"),
            "confirmations":      _confirmations,
            "confirmation_flags": _conf_flags,
            "exit_style":         exit_style,
        }

    # 1. 突破（Breakout）：收盤突破20日高 + 量比≥1.5 + RS百分位≥70 + BULL動能確認
    if high20 and price > high20 and vol_day >= 1.5 and rs_pct >= 70 and _short_ok and _bull_momentum:
        _stop_bk = max(
            low20 or price * 0.95,
            round(avwap_swing * 0.99, 2) if avwap_swing else 0,
        )
        s = _sig("breakout", "突破", "strong", price, _stop_bk,
                 f"收盤({price})突破20日高({high20})，量比{vol_day:.1f}x，RS百分位{rs_pct}")
        if s: signals.append(s)

    # 2. 假跌破（False Breakdown）：昨收跌破前低今日強力收復 + RS≥50
    if low20 and prev_close and prev_low20 and prev_close < prev_low20 and price > low20 and rs_pct >= 50:
        s = _sig("false_breakdown", "假跌破", "medium", price, round(low20 * 0.98, 2),
                 f"昨收({prev_close})跌破前20日低，今收({price})強力收復，RS百分位{rs_pct}")
        if s: signals.append(s)

    # 3a. 均線回測（起漲型）：RS 40~60 + RS斜率剛翻正
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20 and price > 0:
        dist_ma20 = (price - ma20) / ma20
        if 0 <= dist_ma20 <= 0.03 and 40 <= rs_pct < 60 and rs_trend_val is not None and rs_trend_val > 0:
            s = _sig("ma_pullback", "均線回測(起漲型)", "weak", price, round(ma20 * 0.99, 2),
                     f"均線多頭，RS百分位{rs_pct}(40~60)，RS斜率剛翻正")
            if s: signals.append(s)

    # 3b. 均線回測（主升型）：RS≥60
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20 and price > 0:
        dist_ma20 = (price - ma20) / ma20
        if 0 <= dist_ma20 <= 0.03 and rs_pct >= 60:
            s = _sig("ma_pullback", "均線回測(主升型)", "medium", price, round(ma20 * 0.99, 2),
                     f"均線多頭，RS百分位{rs_pct}(≥60)")
            if s: signals.append(s)

    # 4. 強整再突（High Base Breakout）：距20日高≤5% + MA5之上 + RS≥70 + BULL動能確認
    if high20 and ma5 and price > ma5 and rs_pct >= 70 and _short_ok and _bull_momentum:
        dist_high20 = (high20 - price) / high20
        if 0 <= dist_high20 <= 0.05:
            _stop_hb = round(avwap_swing * 0.99, 2) if avwap_swing else (ma10 or round(price * 0.95, 2))
            s = _sig("high_base", "強整再突", "medium", price, _stop_hb,
                     f"緊貼20日高({high20})整理，RS百分位{rs_pct}")
            if s: signals.append(s)

    # 5a. 縮量回測（起漲型）：RS 40~60 + RS斜率翻正 + 縮量
    if ma10 and ma20 and price > ma20:
        dist_ma10 = abs(price - ma10) / ma10
        if dist_ma10 <= 0.02 and vol_day < 1.0 and 40 <= rs_pct < 60 and rs_trend_val is not None and rs_trend_val > 0:
            s = _sig("retest", "縮量回測(起漲型)", "weak", price, round(ma20 * 0.99, 2),
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})，RS百分位{rs_pct}(40~60)，RS斜率剛翻正")
            if s: signals.append(s)

    # 5b. 縮量回測（主升型）：RS≥60 + 縮量
    if ma10 and ma20 and price > ma20:
        dist_ma10 = abs(price - ma10) / ma10
        if dist_ma10 <= 0.02 and vol_day < 1.0 and rs_pct >= 60:
            s = _sig("retest", "縮量回測(主升型)", "medium", price, round(ma20 * 0.99, 2),
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})，RS百分位{rs_pct}(≥60)")
            if s: signals.append(s)

    # 6. MA60支撐：RS≥55 + 收盤距MA60在2%以內
    if ma60 and rs_pct >= 55:
        dist_ma60 = (price - ma60) / ma60
        if 0 <= dist_ma60 <= 0.02:
            s = _sig("ma60_support", "MA60支撐", "weak", price, round(ma60 * 0.97, 2),
                     f"收盤({price})貼近MA60({ma60})，RS百分位{rs_pct}")
            if s: signals.append(s)

    # 7. 趨勢延伸（Trend Continuation）：主升段中繼確認
    # expansion 未帶 structure 時 structure="" → 此訊號不觸發（正確行為）
    _already_covered = any(sig.get("type") in ("breakout", "high_base") for sig in signals)
    if (not _already_covered
            and structure in ("主升段", "主升段✓", "主升段✓✓")
            and stock_phase == "BULL"
            and rs_pct >= 65
            and ma5 and ma10 and ma20
            and ma5 > ma10 > ma20
            and price > ma5
            and high20 and price <= high20 * 1.15
            and (m_z_val is None or m_z_val > 1.0)):
        _stop_tc = max(
            round(ma20, 2),
            round(avwap_swing * 0.99, 2) if avwap_swing else 0,
        )
        _m_str = f"{m_z_val:.2f}" if m_z_val is not None else "N/A"
        s = _sig("trend_cont", "趨勢延伸", "medium", price, _stop_tc,
                 f"主升段均線多頭排列，RS百分位{rs_pct}，M={_m_str}，已站上所有均線✓")
        if s: signals.append(s)

    return signals
