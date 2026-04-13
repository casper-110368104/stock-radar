# ========================================
# execution_model.py (FIXED VERSION)
# ========================================

from dataclasses import dataclass


# ========================================
# 1️⃣ 資料結構
# ========================================

@dataclass
class TradeExecution:
    trigger_price: float
    open_price: float
    entry_price: float | None
    stop_price: float
    target_price: float

    gap_pct: float = 0.0
    gap_type: str = "NORMAL"
    slippage: float = 0.0
    actual_rr: float = 0.0
    entry_score: float = 0.0
    ambiguous_trade: bool = False


# ========================================
# 2️⃣ Gap 計算
# ========================================

def calculate_gap(trigger_price, open_price):
    gap_pct = (open_price - trigger_price) / trigger_price

    if gap_pct > 0.02:
        gap_type = "LARGE_GAP"
    elif gap_pct > 0.005:
        gap_type = "MEDIUM_GAP"
    else:
        gap_type = "NORMAL"

    return gap_pct, gap_type


# ========================================
# 3️⃣ 滑價模型
# ========================================

def get_slippage(gap_pct):
    if gap_pct > 0.02:
        return 0.01
    elif gap_pct > 0.005:
        return 0.005
    else:
        return 0.002


# ========================================
# 4️⃣ Entry（修正：未觸發直接 None）
# ========================================

def calculate_entry_price(trigger_price, open_price):
    # 修正一：未突破 trigger 不進場
    if open_price < trigger_price:
        return None, 0.0, 0.0

    gap_pct = (open_price - trigger_price) / trigger_price
    slippage = get_slippage(gap_pct)
    entry_price = open_price * (1 + slippage)

    return entry_price, gap_pct, slippage


# ========================================
# 5️⃣ Exit（修正：停損滑價）
# ========================================

def calculate_exit_price(price, direction, slippage):
    if direction == "stop":
        return price * (1 - slippage)
    elif direction == "target":
        return price  # 保守：不調整
    return price


# ========================================
# 6️⃣ RR
# ========================================

def calculate_actual_rr(entry_price, stop_price, target_price):
    if entry_price is None:
        return 0

    risk = entry_price - stop_price
    reward = target_price - entry_price

    if risk <= 0:
        return 0

    return reward / risk


# ========================================
# 7️⃣ Entry Score（修正：雙 AVWAP）
# ========================================

def calculate_entry_score(gap_pct, price, ma20, avwap_swing, avwap_vol, volume_ratio):
    score = 0

    # gap penalty
    if gap_pct > 0.02:
        score -= 30
    elif gap_pct > 0.005:
        score -= 10

    # MA20 proximity
    if ma20 and abs(price - ma20) / ma20 < 0.02:
        score += 10

    # AVWAP swing（主趨勢錨）
    if avwap_swing and price > avwap_swing:
        score += 10

    # AVWAP vol（主力成本錨，權重更高）
    if avwap_vol and price > avwap_vol:
        score += 15

    # volume
    if volume_ratio > 1.5:
        score += 10
    elif volume_ratio < 0.8:
        score -= 5

    return score


# ========================================
# 8️⃣ ambiguous
# ========================================

def check_ambiguous(high, low, target, stop):
    return high >= target and low <= stop


# ========================================
# 9️⃣ 主建構
# ========================================

def build_trade_execution(
    trigger_price,
    open_price,
    high,
    low,
    stop_price,
    target_price,
    ma20,
    avwap_swing,
    avwap_vol,
    volume_ratio
):
    entry_price, _, _ = calculate_entry_price(trigger_price, open_price)
    gap_pct, gap_type = calculate_gap(trigger_price, open_price)

    # 未觸發直接回傳
    if entry_price is None:
        return TradeExecution(
            trigger_price=trigger_price,
            open_price=open_price,
            entry_price=None,
            stop_price=stop_price,
            target_price=target_price,
            gap_pct=gap_pct,
            gap_type=gap_type,
        )

    slippage = get_slippage(gap_pct)

    # 停損滑價，目標保守不調整
    adj_stop   = calculate_exit_price(stop_price,   "stop",   slippage)
    adj_target = calculate_exit_price(target_price, "target", slippage)

    actual_rr = calculate_actual_rr(entry_price, adj_stop, adj_target)

    entry_score = calculate_entry_score(
        gap_pct, entry_price, ma20, avwap_swing, avwap_vol, volume_ratio
    )

    ambiguous = check_ambiguous(high, low, adj_target, adj_stop)

    return TradeExecution(
        trigger_price=trigger_price,
        open_price=open_price,
        entry_price=entry_price,
        stop_price=adj_stop,
        target_price=adj_target,
        gap_pct=gap_pct,
        gap_type=gap_type,
        slippage=slippage,
        actual_rr=actual_rr,
        entry_score=entry_score,
        ambiguous_trade=ambiguous,
    )


# ========================================
# 🔟 進場決策（修正：依訊號類型放寬大跳空）
# ========================================

def should_take_trade(execution: TradeExecution, signal_type: str = ""):
    # LARGE_GAP 只允許 breakout / trend_cont（強勢突破常伴隨大跳空）
    if execution.gap_type == "LARGE_GAP":
        if signal_type not in ("breakout", "trend_cont"):
            return False, 0

    # RR 過低直接過濾
    if execution.actual_rr < 1.0:
        return False, 0

    # entry score 分級倉位
    if execution.entry_score < -10:
        return False, 0
    elif execution.entry_score < 10:
        return True, 0.5
    else:
        return True, 1.0
