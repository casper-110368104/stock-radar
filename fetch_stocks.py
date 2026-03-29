"""
台股選股雷達 v7
資料來源分工：
  - TWSE 證交所公開 API：每日量能/漲幅動態名單（免費無限制）
  - Yahoo Finance (yfinance)：股價、基本面、產業、歷史價量
  - FinMind：三大法人籌碼（個股查詢，免費）
  - Gemini：新聞標籤 + 情緒 + 概念題材
"""

import requests, json, time, os, yfinance as yf
from datetime import datetime, timedelta

FINMIND_TOKEN  = os.environ.get("FINMIND_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
OUTPUT_PATH    = "docs/stocks.json"
FINMIND_URL    = "https://api.finmindtrade.com/api/v4/data"
HEADERS        = {"User-Agent": "Mozilla/5.0"}

# 固定必追蹤大型股
# ── 產業輪動：TWSE 類股指數對照表 ────────────────────────
SECTOR_MAP = {
    "發行量加權股價指數": "_benchmark",
    "半導體類指數":       "半導體",
    "電子工業類指數":     "電子工業",
    "電腦及週邊設備類指數": "電腦週邊",
    "光電類指數":         "光電",
    "通信網路類指數":     "通信網路",
    "電子零組件類指數":   "電子零組件",
    "電子通路類指數":     "電子通路",
    "資訊服務類指數":     "資訊服務",
    "其他電子類指數":     "其他電子",
    "金融保險類指數":     "金融保險",
    "建材營造類指數":     "建材營造",
    "航運類指數":         "航運",
    "觀光餐旅類指數":     "觀光餐旅",
    "貿易百貨類指數":     "貿易百貨",
    "油電燃氣類指數":     "油電燃氣",
    "綠能環保類指數":     "綠能環保",
    "電機機械類指數":     "電機機械",
    "生技醫療類指數":     "生技醫療",
    "鋼鐵類指數":         "鋼鐵",
    "汽車類指數":         "汽車",
    "食品類指數":         "食品",
    "紡織纖維類指數":     "紡織",
    "水泥類指數":         "水泥",
    "塑膠類指數":         "塑膠",
    "電器電纜類指數":     "電器電纜",
    "化學類指數":         "化學",
    "化學生技醫療類指數": "化學生技",
    "玻璃陶瓷類指數":     "玻璃陶瓷",
    "造紙類指數":         "造紙",
    "橡膠類指數":         "橡膠",
    "數位雲端類指數":     "數位雲端",
    "運動休閒類指數":     "運動休閒",
    "居家生活類指數":     "居家生活",
    "其他類指數":         "其他",
}

_sector_rotation = {}  # 產業輪動四象限資料


def _compute_sector_breadth(twse_stocks):
    """從 STOCK_DAY_ALL 資料計算每產業上漲比例 {sector_key: 0~1}
    只計算 STOCK_SECTOR_MAP 有明確對照的個股（靜態表，覆蓋主要成分股）
    """
    from collections import defaultdict
    totals = defaultdict(int)
    ups    = defaultdict(int)
    for s in twse_stocks:
        sk = STOCK_SECTOR_MAP.get(s["code"], "")
        if sk:
            totals[sk] += 1
            if s["chg"] > 0:
                ups[sk] += 1
    return {sk: round(ups[sk] / totals[sk], 3) for sk in totals if totals[sk] >= 5}


def fetch_sector_rotation(days=60, breadth_map=None):
    """從 TWSE MI_INDEX API 抓類股指數歷史，計算 RS/M/A/Score，回傳六象限資料
    RS           = 産業漲幅 - 大盤漲幅（日報酬差，%）
    M            = RS / RS_MA10（動能，無地板；RS_MA10=0 時為 0）
    A            = M_today - M_3day_avg（加速度，與個股公式一致）
    RS_trend     = RS 的5日線性斜率
    Breadth      = 上漲家數 / 總家數（當日 snapshot，由 breadth_map 傳入）
    Score        = 0.4*RS_rank + 0.3*M_rank + 0.2*A_rank + 0.1*Breadth（跨産業百分位）
    Trend_struct = 1 if price > MA60 else -1
    六象限：主升段 / 準備噴 / 主升回檔 / 高檔震盪 / 空頭 / 整理觀察
    """
    from datetime import datetime, timedelta
    daily_data = {}
    days_back = 0
    collected = 0
    today = datetime.now()
    empty_streak = 0

    print(f"  [產業輪動] 開始抓取近 {days} 個交易日...")
    while collected < days and days_back < days * 2 and empty_streak < 10:
        days_back += 1
        d = today - timedelta(days=days_back)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y%m%d")
        try:
            url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&type=IND&date={date_str}"
            res = requests.get(url, headers=HEADERS, timeout=12)
            data = res.json()
            if data.get("stat") != "OK":
                empty_streak += 1
                continue
            table = next((t for t in data.get("tables", []) if t.get("data")), None)
            if not table:
                empty_streak += 1
                continue
            day_dict = {}
            for row in table["data"]:
                name = row[0]
                if name in SECTOR_MAP:
                    try:
                        close = float(row[1].replace(",", ""))
                        day_dict[SECTOR_MAP[name]] = close
                    except Exception:
                        pass
            if "_benchmark" in day_dict and len(day_dict) > 10:
                daily_data[d.strftime("%Y-%m-%d")] = day_dict
                collected += 1
                empty_streak = 0
                if collected % 10 == 0:
                    print(f"  [產業輪動] 已收集 {collected}/{days} 天")
            else:
                empty_streak += 1
            time.sleep(0.25)
        except Exception as e:
            empty_streak += 1
            time.sleep(0.5)
            continue

    print(f"  [產業輪動] 收集完成：{collected} 個交易日，{len(daily_data)} 筆有效資料")
    if not daily_data:
        print("  [產業輪動] 無資料，跳過")
        return {}

    dates = sorted(daily_data.keys())
    sectors = [v for v in SECTOR_MAP.values() if v != "_benchmark"]
    rs_history = {s: [] for s in sectors}
    price_history = {s: [] for s in sectors}

    # 收集原始價格序列（供 MA60 / Trend_structure 使用）
    for date in dates:
        day = daily_data[date]
        for s in sectors:
            if s in day:
                price_history[s].append(day[s])

    # RS = 産業日漲幅 - 大盤日漲幅（需要相鄰兩日）
    for i in range(1, len(dates)):
        date = dates[i]
        prev_date = dates[i - 1]
        day = daily_data[date]
        prev_day = daily_data.get(prev_date, {})
        bm = day.get("_benchmark", 0)
        prev_bm = prev_day.get("_benchmark", 0)
        if bm <= 0 or prev_bm <= 0:
            continue
        bm_ret = (bm / prev_bm - 1) * 100
        for s in sectors:
            if s in day and s in prev_day and prev_day[s] > 0:
                sector_ret = (day[s] / prev_day[s] - 1) * 100
                rs = round(sector_ret - bm_ret, 4)
                rs_history[s].append({"date": date[5:], "rs": rs})

    def linear_slope(vals):
        n = len(vals)
        if n < 2:
            return 0.0
        x_mean = (n - 1) / 2
        y_mean = sum(vals) / n
        num = sum((i - x_mean) * (vals[i] - y_mean) for i in range(n))
        den = sum((i - x_mean) ** 2 for i in range(n))
        return num / den if den else 0.0

    # ── 第一階段：逐産業計算 RS/M/A/rs_trend/trend_struct ──────────
    min_required = 13  # 需要 10(RS_MA10) + 3(A計算) 天
    raw = {}           # sector → 中間資料
    for s in sectors:
        hist = rs_history[s]
        if len(hist) < min_required:
            continue
        rs_vals = [h["rs"] for h in hist]

        # RS_MA10 序列
        rs_ma10_series = [None] * len(rs_vals)
        for i in range(9, len(rs_vals)):
            rs_ma10_series[i] = sum(rs_vals[i-9:i+1]) / 10

        # M = RS / RS_MA10（無地板，RS_MA10=0 時設為 0）
        m_series = [None] * len(rs_vals)
        for i in range(len(rs_vals)):
            if rs_ma10_series[i] is not None:
                m_series[i] = rs_vals[i] / rs_ma10_series[i] if rs_ma10_series[i] != 0 else 0.0

        # A = M_today - M_3day_avg（取最後3個有效 M 值）
        m_tail = [v for v in m_series[-3:] if v is not None]
        today_a = (m_tail[-1] - sum(m_tail) / len(m_tail)) if len(m_tail) >= 3 else None

        today_rs = rs_vals[-1]
        today_m  = m_series[-1]
        if today_m is None or today_a is None:
            continue

        # RS_trend：最近5日RS線性斜率
        rs_trend = round(linear_slope(rs_vals[-5:]), 4)

        # Trend_structure：價格 vs MA60
        prices = price_history[s]
        ma60 = sum(prices) / len(prices) if prices else None
        trend_struct = (1 if prices[-1] > ma60 else -1) if (ma60 and prices) else 0

        # 連漲天數（RS連續上升）
        rs_up_days = 0
        for i in range(len(rs_vals) - 1, 0, -1):
            if rs_vals[i] > rs_vals[i - 1]:
                rs_up_days += 1
            else:
                break

        prev_rs = rs_vals[-2] if len(rs_vals) >= 2 else today_rs
        prev_m  = m_series[-2] if (len(m_series) >= 2 and m_series[-2] is not None) else today_m
        raw[s] = {
            "rs":          today_rs,
            "m":           today_m,
            "a":           today_a,
            "rs_trend":    rs_trend,
            "trend_struct": trend_struct,
            "rs_up_days":  rs_up_days,
            "vector":      {"dx": round(today_rs - prev_rs, 4), "dy": round(today_m - prev_m, 4)},
            "trend":       [{"date": h["date"], "rs": h["rs"]} for h in hist[-15:]],
        }

    # ── 第二階段：跨産業 rank → Score → 六象限 ────────────────────
    def _cross_rank(items):
        """items = [(sector, value), ...]，回傳 {sector: 0~1 百分位}"""
        n = len(items)
        if n == 0:
            return {}
        sorted_items = sorted(items, key=lambda x: x[1])
        return {s: round(i / (n - 1), 4) if n > 1 else 0.5
                for i, (s, _) in enumerate(sorted_items)}

    valid_sectors = list(raw.keys())
    rs_rank = _cross_rank([(s, raw[s]["rs"]) for s in valid_sectors])
    m_rank  = _cross_rank([(s, raw[s]["m"])  for s in valid_sectors])
    a_rank  = _cross_rank([(s, raw[s]["a"])  for s in valid_sectors])

    result = {}
    for s in valid_sectors:
        d        = raw[s]
        breadth  = (breadth_map or {}).get(s)          # None 若無資料
        b_val    = breadth if breadth is not None else 0.5  # 無資料時用中性值
        score    = round(0.4 * rs_rank[s] + 0.3 * m_rank[s] + 0.2 * a_rank[s] + 0.1 * b_val, 3)

        # 六象限：以 Score + trend_struct + rs_trend 決定
        sc = score; ts = d["trend_struct"]; rt = d["rs_trend"]
        if   sc >= 0.7 and ts > 0 and rt > 0:
            sub_phase = "主升段"      # 高分 + 多頭 + 趨勢向上
        elif sc >= 0.6 and ts < 0 and rt > 0:
            sub_phase = "準備噴"      # 高分 + 空頭結構 + RS剛翻升（底部轉強）
        elif 0.4 <= sc < 0.7 and ts > 0 and rt > 0:
            sub_phase = "主升回檔"    # 中分 + 多頭 + 趨勢仍上
        elif 0.4 <= sc < 0.7 and ts > 0 and rt <= 0:
            sub_phase = "高檔震盪"    # 中分 + 多頭 + 趨勢轉弱
        elif sc < 0.3 and ts < 0:
            sub_phase = "空頭"        # 低分 + 空頭結構
        else:
            sub_phase = "整理觀察"    # 過渡中性

        result[s] = {
            "rs":           round(d["rs"], 4),
            "rs_mom":       round(d["m"],  4),   # key 保持 rs_mom（前端相容）
            "acceleration": round(d["a"],  4),
            "rs_trend":     d["rs_trend"],
            "breadth":      breadth,              # None 若無法計算
            "score":        score,
            "sub_phase":    sub_phase,
            "rs_up_days":   d["rs_up_days"],
            "vector":       d["vector"],
            "trend":        d["trend"],
        }

    phase_counts = {}
    for v in result.values():
        phase_counts[v["sub_phase"]] = phase_counts.get(v["sub_phase"], 0) + 1
    summary = " | ".join(f"{k}:{v}" for k, v in sorted(phase_counts.items()))
    print(f"  [產業輪動] {len(result)} 個產業 | {summary}")
    return result


LARGE_CAP = {
    "2330","2454","2317","2382","2308","2303","2412",
    "2881","2882","2883","2884","2885","2886","2891","2892",
    "1301","1303","1326","2002","2207",
    "2357","2395","2408","2474","3008","3034","3045",
    "3711","4938","5871","6415","6505","6669","8046",
}

def get_date(days_ago=0):
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

def get_twse_date(days_ago=0):
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")


# ── 1. TWSE 動態名單（量能前100 + 漲幅前50 + 跌幅前30）──────────

def fetch_twse_dynamic():
    """用 TWSE 公開 API 抓全市場當日資料，取量能/漲跌幅前N名
    回傳 (merged_codes, all_stocks)：
      merged_codes = 量能/漲跌幅精選代碼清單
      all_stocks   = 全市場 [{code, vol, chg}, ...] 供 breadth 計算
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        d = res.json()
        if d.get("stat") != "OK":
            print(f"  [TWSE] 狀態異常: {d.get('stat')}")
            return [], []

        fields = d.get("fields", [])
        rows   = d.get("data", [])

        # 欄位索引：證券代號/名稱/成交股數/收盤/漲跌
        # 通常欄位：['證券代號','證券名稱','成交股數','成交筆數','成交金額','開盤價','最高價','最低價','收盤價','漲跌(+/-)','漲跌價差','最後揭示買價','最後揭示買量','最後揭示賣價','最後揭示賣量','本益比']
        try:
            i_code = fields.index("證券代號")
            i_name = fields.index("證券名稱")
            i_vol  = fields.index("成交股數")
            i_cls  = fields.index("收盤價")
            i_open = fields.index("開盤價")
        except ValueError:
            i_code, i_name, i_vol, i_open, i_cls = 0, 1, 2, 5, 8

        stocks = []
        for r in rows:
            try:
                code = r[i_code].strip()
                if not (code.isdigit() and len(code) == 4):
                    continue
                vol  = int(r[i_vol].replace(",",""))
                cls  = float(r[i_cls].replace(",","")) if r[i_cls] not in ("--","") else 0
                opn  = float(r[i_open].replace(",","")) if r[i_open] not in ("--","") else cls
                chg  = (cls - opn) / opn * 100 if opn > 0 else 0
                name = r[i_name].strip() if i_name < len(r) else ""
                if name and code not in _name_cache:
                    _name_cache[code] = (name, "其他")
                stocks.append({"code": code, "vol": vol, "chg": chg})
            except Exception:
                continue

        if not stocks:
            print("  [TWSE] 解析後無資料")
            return [], []

        by_vol  = sorted(stocks, key=lambda x: x["vol"], reverse=True)
        by_rise = sorted(stocks, key=lambda x: x["chg"], reverse=True)
        by_fall = sorted(stocks, key=lambda x: x["chg"])

        top_vol  = [s["code"] for s in by_vol[:100]]
        top_rise = [s["code"] for s in by_rise[:50]]
        top_fall = [s["code"] for s in by_fall[:30]]

        merged = list(dict.fromkeys(top_vol + top_rise + top_fall))
        print(f"  [TWSE] 量能{len(top_vol)} 漲幅{len(top_rise)} 跌幅{len(top_fall)} → {len(merged)} 檔，名稱快取{len(_name_cache)}檔")
        return merged, stocks

    except Exception as e:
        print(f"  [TWSE] 失敗：{e}")
        return [], []


# ── 2. Yahoo Finance：股價 + 基本面 + 歷史價量 ────────────────────

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
    """計算個股日 RS 序列 = 個股日報酬 − 基準日報酬（單位：%）
    與類股輪動 rs_history[s] 同頻率、同單位，可直接比較 M 值。
    只需 15 個交易日即可計算（原本需 240 日）。
    """
    n = min(len(sc), len(bc))
    sc, bc = sc[-n:], bc[-n:]
    daily_rs = []
    for t in range(1, n):
        if sc[t-1] > 0 and bc[t-1] > 0:
            sr = sc[t] / sc[t-1] - 1
            br = bc[t] / bc[t-1] - 1
            daily_rs.append(round((sr - br) * 100, 4))  # % 單位，與類股一致
    return daily_rs


def _compute_m_a(daily_rs):
    """個股動能計算，公式與類股輪動完全一致：
    M = 日RS / MA10(日RS)   —  >1 表示 RS 高於10日均（動能加速）
    A = M今日 - avg(M近3日) —  正值代表動能加速，負值代表動能減速
    rs_trend = 日RS 的5日線性斜率（偵測真正的近期方向）
    最低需要 13 筆日 RS（10日MA + 3日A計算）。

    對齊確認（與 fetch_sector_rotation 完全一致）：
      RS      : 日報酬差（%） ✓
      M       : RS / MA10(RS) ✓
      A       : M今日 − avg(M近3日) ✓
      rs_trend: 5日線性斜率 ✓
    """
    if len(daily_rs) < 13:
        return None, None, None

    # M 序列（與類股 m_series 邏輯相同）
    m_series = []
    for i in range(9, len(daily_rs)):
        rs_ma10 = sum(daily_rs[i-9:i+1]) / 10
        m = (daily_rs[i] / rs_ma10) if rs_ma10 != 0 else 0.0
        m = max(-5.0, min(5.0, m))   # RS_MA10 趨近零時截斷，避免極端值
        m_series.append(m)

    if len(m_series) < 3:
        return None, None, None

    m_today = m_series[-1]

    # A = M今日 - avg(M近3日)（與類股 today_a 公式相同）
    m_tail = m_series[-3:]
    a = m_tail[-1] - sum(m_tail) / len(m_tail)

    # rs_trend：日RS 5日線性斜率（真正的近期方向，非原本60日窗口斜率）
    if len(daily_rs) >= 5:
        vals   = daily_rs[-5:]
        mu5    = sum(vals) / 5
        x_mean = 2.0
        num    = sum((i - x_mean) * (vals[i] - mu5) for i in range(5))
        den    = sum((i - x_mean) ** 2 for i in range(5))
        rs_trend = round(num / den, 4) if den else 0.0
    else:
        rs_trend = None

    return round(m_today, 4), round(a, 4), rs_trend


def classify_stock_phase(rs_pct, m_z, a_z, rs_trend, rs_slow_positive=None):
    """依 RS 百分位、M 比值和 RS_trend 分類個股型態。
    m_z 現在是 RS/MA10 比值（M）：M > 1.0 = RS 高於10日均（動能加速），M < 1.0 = 動能減速。
    """
    if rs_pct is None or m_z is None:
        return "RANGE"
    rs_slow_ok = rs_slow_positive if rs_slow_positive is not None else True
    # BULL：RS強 + 動能加速（M>1）+ 加速度正 + 慢速趨勢為正
    if rs_pct >= 70 and m_z > 1.0 and (a_z is None or a_z >= 0) and rs_slow_ok:
        return "RANGE" if (rs_trend is not None and rs_trend < 0) else "BULL"
    # BULL_PULLBACK：RS仍強 + 動能減速（M<1）+ 加速度負 + 趨勢仍向上
    if rs_pct >= 60 and m_z < 1.0 and (a_z is None or a_z < 0) and (rs_trend is None or rs_trend > 0):
        return "BULL_PULLBACK"
    # BEAR_STRONG：RS弱 + 動能減速 + 加速度負
    if rs_pct < 30 and m_z < 1.0 and (a_z is None or a_z < 0):
        return "BEAR_STRONG"
    # BEAR_WEAK：RS偏弱但動能加速（可能底部轉折）
    if rs_pct < 50 and m_z > 1.0:
        return "BEAR_WEAK"
    return "RANGE"


# ── 大盤相位 ──────────────────────────────────────────────────────

_market_regime = {}  # 全域：大盤多空狀態


def fetch_market_regime():
    """抓取加權指數(^TWII) 判斷大盤多空相位（bull/bull_pullback/range/bear）"""
    try:
        tw   = yf.Ticker("^TWII")
        hist = tw.history(period="90d")
        if hist.empty or len(hist) < 60:
            return {"regime": "range", "taiex": None, "ma20": None, "ma60": None}
        closes = hist["Close"].tolist()
        price  = closes[-1]
        ma20   = round(sum(closes[-20:]) / 20, 1)
        ma60   = round(sum(closes[-60:]) / 60, 1)
        if price > ma20 and price > ma60:
            regime = "bull"
        elif price > ma60 and price <= ma20:
            regime = "bull_pullback"
        elif price < ma60 * 0.97:
            regime = "bear"
        else:
            regime = "range"
        return {"regime": regime, "taiex": round(price, 1), "ma20": ma20, "ma60": ma60}
    except Exception as e:
        print(f"  [大盤相位] 失敗：{e}")
        return {"regime": "range", "taiex": None, "ma20": None, "ma60": None}


def classify_structure(yahoo, stock_phase, sector_phase=""):
    """
    根據 MA 排列、價格位置、RS 相位、AVWAP 位置給出人類可讀的結構標籤。
    返回：'主升段'/'主升段✓'/'主升段✓✓' | '突破準備'/'突破準備✓'/'突破準備✓✓'
          | '回檔' | '盤整' | '弱勢'
    ✓  = 類股同步主升；✓✓ = 類股同步 + 三條 AVWAP 全對齊（最強確認）
    AVWAP swing 跌破時，主升/突破降為回檔。
    """
    price  = yahoo.get("price") or 0
    ma5    = yahoo.get("ma5")
    ma10   = yahoo.get("ma10")
    ma20   = yahoo.get("ma20")
    ma60   = yahoo.get("ma60")
    high20 = yahoo.get("high20")
    rs_pct = yahoo.get("rs_pct_val")  # main() 補入
    avwap_swing = yahoo.get("avwap_swing")
    avwap_vol   = yahoo.get("avwap_vol")
    avwap_short = yahoo.get("avwap_short")

    if not price or not ma20:
        return "盤整"

    # 弱勢：空頭排列或跌破 MA60
    if stock_phase in ("BEAR_STRONG", "BEAR_WEAK") or (ma60 and price < ma60 * 0.98):
        label = "弱勢"
    # 主升段：多頭排列 + 股價在 MA5 之上
    elif (stock_phase == "BULL" and ma5 and ma10 and ma20
          and ma5 > ma10 > ma20 and price >= ma5):
        label = "主升段"
    # 突破準備：股價距20日高點在10%以內 + 高於MA20 + RS>=65
    elif (high20 and ma20 and price >= ma20
          and 0 <= (high20 - price) / high20 <= 0.10
          and (rs_pct is None or rs_pct >= 65)):
        label = "突破準備"
    # 回檔：多頭排列但股價在 MA20~MA10 之間
    elif (stock_phase in ("BULL", "BULL_PULLBACK")
          and ma10 and ma20 and ma20 <= price <= ma10):
        label = "回檔"
    else:
        label = "盤整"

    # AVWAP 強化：三條全對齊 → 最強確認（加 ✓✓）；swing 跌破 → 主升/突破降為回檔
    _avwap_all_ok = (
        avwap_swing and avwap_vol and avwap_short
        and price >= avwap_swing and price >= avwap_vol and price >= avwap_short
    )
    _avwap_broken = avwap_swing and price < avwap_swing

    if _avwap_broken and label in ("主升段", "突破準備"):
        label = "回檔"   # 趨勢 AVWAP 跌破，結構降級

    # 類股相位加分/降級
    if label in ("主升段", "突破準備"):
        if sector_phase in ("主升段", "準備噴", "主升回檔"):
            if _avwap_all_ok:
                label = label + "✓✓"  # 類股 + AVWAP 三線全對齊，最強
            else:
                label = label + "✓"   # 類股確認
        elif sector_phase == "空頭":
            label = "盤整"             # 類股逆風，降級
        elif _avwap_all_ok:
            label = label + "✓"        # 無類股確認但 AVWAP 三線對齊，仍值得標記

    return label


def calc_atr(highs, lows, closes, period=14):
    """計算近 period 日 Average True Range"""
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    return round(sum(trs[-period:]) / period, 2)


def fetch_yahoo(sid):
    """用 yfinance 抓單股資料，回傳 dict"""
    result = {
        "price": None, "change": None, "change_pct": None,
        "volume": None, "vol_day_ratio": None, "vol_month_ratio": None,
        "week52_high": None, "week52_low": None,
        "name": sid, "industry": "其他",
        "eps_ttm": None, "eps_growth": None,
        "roe": None, "debt_ratio": None,
        "pe_low": None, "pe_mid": None, "pe_high": None,
        "ma60": None, "above_ma60": None, "rs": None,
        "size_cat": "mid", "market_cap_b": 0,
        "vol20_avg": None, "vol60_avg": None, "shares_outstanding": 0,
        "open": None, "high": None, "low": None,
        "prev_close": None, "prev_high": None, "prev_low": None,
        "ma5": None, "ma10": None, "ma20": None, "ma_bullish": None,
        "high20": None, "low20": None, "prev_low20": None,
    }
    try:
        ticker = yf.Ticker(f"{sid}.TW")
        info   = ticker.info or {}

        # 股價（台股單位是 TWD，Yahoo 給的就是台幣股價）
        price = (info.get("currentPrice") or info.get("regularMarketPrice") or
                 info.get("ask") or info.get("bid"))
        prev  = info.get("previousClose") or info.get("regularMarketPreviousClose")
        if price and prev and float(price) > 0:
            result["price"]      = round(float(price), 2)
            result["change"]     = round(float(price) - float(prev), 2)
            result["change_pct"] = round((float(price) - float(prev)) / float(prev) * 100, 2)
        # 從歷史資料拿最新收盤（備用，當 info 沒有 currentPrice 時）

        # 名稱先用 Yahoo，之後會被 FinMind 中文覆蓋
        result["name"]     = info.get("shortName") or info.get("longName") or sid
        result["_quote_type"] = info.get("quoteType", "")
        result["industry"] = info.get("industry") or info.get("sector") or "其他"

        # 市值動態分類
        mc_b = (info.get("marketCap") or 0) / 1e8
        if   mc_b >= 1000: result["size_cat"] = "large"
        elif mc_b >=  100: result["size_cat"] = "mid"
        else:              result["size_cat"] = "small"
        result["market_cap_b"] = round(mc_b, 0)

        # 52週
        result["week52_high"] = info.get("fiftyTwoWeekHigh")
        result["week52_low"]  = info.get("fiftyTwoWeekLow")
        result["shares_outstanding"] = info.get("sharesOutstanding") or 0

        # EPS TTM：優先用 trailingEps
        eps = info.get("trailingEps")
        if eps:
            result["eps_ttm"] = round(float(eps), 2)

        # ROE：yfinance 給小數（0.25 = 25%），也有可能是 None
        roe = info.get("returnOnEquity")
        if roe is not None:
            result["roe"] = round(float(roe) * 100, 1)

        # 負債比：優先用 totalDebt/totalAssets，備用 debtToEquity 換算
        total_debt   = float(info.get("totalDebt",   0) or 0)
        total_assets = float(info.get("totalAssets", 0) or 0)
        if total_assets > 0 and total_debt > 0:
            result["debt_ratio"] = round(total_debt / total_assets * 100, 1)
        else:
            # debtToEquity = D/E 比（%），換算負債比 = D/(D+E) = (D/E) / (1 + D/E)
            de = info.get("debtToEquity")
            if de is not None and float(de) >= 0:
                de_ratio = float(de) / 100  # Yahoo 給的是百分比形式
                result["debt_ratio"] = round(de_ratio / (1 + de_ratio) * 100, 1)

        # EPS 年增率：
        # earningsGrowth = 季增率（YoY），是小數，可能 None
        # earningsQuarterlyGrowth 同上
        # 優先用 earningsGrowth，乘100轉百分比
        for eg_key in ("earningsGrowth", "earningsQuarterlyGrowth"):
            eg = info.get(eg_key)
            if eg is not None:
                raw = float(eg) * 100
                # 極端值壓縮：超過±150%視為異常，壓縮到±80%
                if raw > 150:   raw = 80 + (raw - 150) * 0.1
                elif raw < -80: raw = -60
                result["eps_growth"] = round(raw, 1)
                break

        # 歷史價量（月量比 + 歷史PE）
        hist = ticker.history(period="2y")
        if len(hist) >= 20:
            vols = hist["Volume"].tolist()
            closes = hist["Close"].tolist()
            today_vol = vols[-1] if vols else 0
            result["volume"] = int(today_vol)

            # 月量比：近20日均量 vs 前20~80日均量
            recent20 = vols[-20:]
            prev_vols = vols[-80:-20] if len(vols) >= 80 else vols[:-20]
            avg_recent = sum(recent20) / len(recent20) if recent20 else 0
            avg_prev   = sum(prev_vols) / len(prev_vols) if prev_vols else 0
            result["vol_month_ratio"] = round(avg_recent / avg_prev, 2) if avg_prev > 0 else 1.0
            result["vol_day_ratio"]   = round(today_vol  / avg_recent, 2) if avg_recent > 0 else 1.0
            # vol20/vol60
            if len(vols) >= 60:
                result["vol20_avg"] = sum(vols[-20:]) / 20
                result["vol60_avg"] = sum(vols[-60:]) / 60

            # OHLC + prev_close（供訊號計算用）
            highs  = hist["High"].tolist()
            lows   = hist["Low"].tolist()
            opens  = hist["Open"].tolist()
            if len(closes) >= 2:
                result["open"]       = round(opens[-1],  2)
                result["high"]       = round(highs[-1],  2)
                result["low"]        = round(lows[-1],   2)
                result["prev_close"] = round(closes[-2], 2)
                result["prev_high"]  = round(highs[-2],  2)
                result["prev_low"]   = round(lows[-2],   2)

            # MA5, MA10, MA20
            if len(closes) >= 20:
                result["ma5"]  = round(sum(closes[-5:])  / 5,  2)
                result["ma10"] = round(sum(closes[-10:]) / 10, 2)
                result["ma20"] = round(sum(closes[-20:]) / 20, 2)
                result["ma_bullish"] = (result["ma5"] > result["ma10"] > result["ma20"])

            # 20日高低點（不含當日，供突破/假跌破訊號用）
            if len(closes) >= 21:
                result["high20"]     = round(max(highs[-21:-1]), 2)
                result["low20"]      = round(min(lows[-21:-1]),  2)
                result["prev_low20"] = round(min(lows[-22:-2]),  2) if len(lows) >= 22 else result["low20"]

            # MA60
            if len(closes) >= 60:
                ma60 = sum(closes[-60:]) / 60
                result["ma60"] = round(ma60, 2)
                result["above_ma60"] = (closes[-1] >= ma60)

            # ATR 14日（動態停損參考）
            result["atr_14"] = calc_atr(highs, lows, closes, 14)

            # 暫存收盤序列供 RS 計算 + 回測
            result["_closes"]  = closes
            result["_highs"]   = highs
            result["_lows"]    = lows
            result["_volumes"] = vols
            result["_opens"]   = opens

            # ── Anchored VWAP 三線 ─────────────────────────
            _n = len(closes)

            # avwap_swing：60日最低點，需後續有結構轉強（price>MA20 或量比>1）
            _base60   = max(0, _n - 60)
            _idx_swng = _base60 + lows[_base60:].index(min(lows[_base60:]))
            _avwap_swing = None
            for _j in range(_idx_swng + 1, _n):
                _win  = closes[max(0, _j-19):_j+1]
                _ma20 = sum(_win) / len(_win)
                _vwin = vols[max(0, _j-19):_j+1]
                _v20  = sum(_vwin) / len(_vwin) if _vwin else 1
                if closes[_j] > _ma20 or vols[_j] / _v20 > 1:
                    _avwap_swing = calc_avwap(closes, highs, lows, vols, _idx_swng)
                    break

            # avwap_vol：20日最大量那天，需收盤>MA20（排除出貨）
            _base20  = max(0, _n - 20)
            _idx_vol = _base20 + vols[_base20:].index(max(vols[_base20:]))
            _vwin20  = closes[max(0, _idx_vol-19):_idx_vol+1]
            _ma20_v  = sum(_vwin20) / len(_vwin20)
            _avwap_vol = calc_avwap(closes, highs, lows, vols, _idx_vol) if closes[_idx_vol] > _ma20_v else None

            # avwap_short：近20日最近一個局部低點+後3日有反彈確認
            _avwap_short = None
            for _j in range(_n - 2, _base20 - 1, -1):
                if _j < 1:
                    break
                if lows[_j] > lows[_j-1] or lows[_j] > lows[min(_j+1, _n-1)]:
                    continue
                _ahead = closes[_j+1:min(_j+4, _n)]
                if len(_ahead) >= 2 and sum(1 for c in _ahead if c > lows[_j]) >= 2:
                    _avwap_short = calc_avwap(closes, highs, lows, vols, _j)
                    break

            result["avwap_swing"] = _avwap_swing
            result["avwap_vol"]   = _avwap_vol
            result["avwap_short"] = _avwap_short

            # 歷史PE分位數（近1年收盤/EPS TTM）
            if result["eps_ttm"] and result["eps_ttm"] > 0:
                pes = [c / result["eps_ttm"] for c in closes
                       if c > 0 and c / result["eps_ttm"] < 200]  # 過濾異常PE
                if len(pes) >= 20:
                    pes_s = sorted(pes); n = len(pes_s)
                    result["pe_low"]  = round(pes_s[int(n*0.15)], 1)
                    result["pe_mid"]  = round(pes_s[int(n*0.50)], 1)
                    result["pe_high"] = round(pes_s[int(n*0.85)], 1)

        # Debug log
        print(f"      Yahoo: price={result['price']} eps={result['eps_ttm']} "
              f"eg={result['eps_growth']} roe={result['roe']} dr={result['debt_ratio']}")

    except Exception as e:
        print(f"    [Yahoo] {sid} 失敗：{e}")

    # 若 yfinance 判斷為 ETF/Fund 類型則標記
    quote_type = result.get("_quote_type", "")
    if quote_type in ("ETF", "MUTUALFUND"):
        result["_is_etf"] = True

    return result


# ── 3. FinMind：三大法人籌碼 ──────────────────────────────────────

def fetch_shareholder(sid):
    return None  # 持股比功能已停用


def fetch_futures_oi():
    """抓外資台指期未平倉口數（大盤指標，非個股）"""
    try:
        params = {
            "dataset":    "TaiwanFuturesInstitutionalInvestors",
            "data_id":    "TX",
            "start_date": get_date(5),
            "end_date":   get_date(),
            "token":      FINMIND_TOKEN,
        }
        res = requests.get(FINMIND_URL, params=params, timeout=15)
        d = res.json()
        if d.get("status") != 200:
            print(f"  [期貨OI] 失敗：{d.get('msg','')}")
            return None
        rows = d.get("data", [])
        # 找最新日期的外資那筆
        rows.sort(key=lambda x: x.get("date",""))
        foreign_rows = [r for r in rows
                        if "外資" in r.get("institutional_investors","")
                        and "自營" not in r.get("institutional_investors","")]
        if not foreign_rows:
            print("  [期貨OI] 找不到外資資料")
            return None
        latest = foreign_rows[-1]
        long_oi  = int(latest.get("long_open_interest_balance_volume",  0) or 0)
        short_oi = int(latest.get("short_open_interest_balance_volume", 0) or 0)
        net      = long_oi - short_oi
        date     = latest.get("date","")
        print(f"  [期貨OI] {date} 外資多單={long_oi} 空單={short_oi} 淨={net}")
        return {"date": date, "long": long_oi, "short": short_oi, "net": net}
    except Exception as e:
        print(f"  [期貨OI] 例外：{e}")
        return None


def fetch_securities_lending(sid):
    """從 TWSE 快取取借券資料（不耗 FinMind 額度）"""
    return get_lending_from_cache(sid)


# 全域快取
_name_cache    = {}  # {sid: (name, industry)}
_lending_cache         = {}  # {sid: {volume, amount, balance}}
_lending_history_cache = {}  # {sid: [{date, volume, balance}, ...]}
_benchmark_closes      = {}  # {large/mid/small: [收盤價...]}
_revenue_map           = {}  # {sid: 月營收YoY%}

def fetch_mops_revenue():
    """
    抓全市場月營收 YoY，回傳 {sid: yoy%}，不消耗 FinMind 額度。
    主力：TWSE / TPEx OpenData JSON API（穩定）
    備用：MOPS 舊靜態 HTML（若 OpenData 失效）
    """
    from datetime import datetime
    from html.parser import HTMLParser
    now = datetime.now()
    # 上個月
    year_ad = now.year
    month   = now.month - 1
    if month == 0:
        month = 12; year_ad -= 1
    year_roc = year_ad - 1911  # 民國年

    result = {}

    # ── 主力：TWSE / TPEx OpenData JSON ───────────────────────────
    apis = [
        # 上市 (SII)
        (f"https://opendata.twse.com.tw/v1/opendata/t187ap03_L", "上市"),
        # 上櫃 (OTC) — TPEx open data
        (f"https://opendata.tpex.org.tw/api/tpex_mainboard_monthly_revenue?"
         f"d={year_roc:03d}/{month:02d}", "上櫃"),
    ]
    for url, mkt in apis:
        try:
            res = requests.get(url, headers=HEADERS, timeout=20)
            if res.status_code != 200:
                print(f"  [月營收] {mkt} OpenData HTTP {res.status_code}，略過")
                continue
            rows = res.json()
            if not isinstance(rows, list):
                rows = rows.get("data", rows.get("Data", []))
            ok = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                # 欄位名稱可能是中文或英文
                code = (row.get("公司代號") or row.get("Code") or "").strip()
                if not (code.isdigit() and len(code) == 4):
                    continue
                yoy_raw = (row.get("去年同月增減(%)") or row.get("YoY(%)") or
                           row.get("yoy") or row.get("MoM%") or "")
                try:
                    yoy = round(float(str(yoy_raw).replace(",", "").replace("+", "").strip()), 1)
                    result[code] = yoy
                    ok += 1
                except (ValueError, TypeError):
                    continue
            print(f"  [月營收] {mkt} OpenData {year_roc}/{month} → {ok} 檔")
        except Exception as e:
            print(f"  [月營收] {mkt} OpenData 失敗：{e}")

    if result:
        return result

    # ── 備用：MOPS 靜態 HTML（舊路徑）────────────────────────────
    print("  [月營收] OpenData 全部失敗，改用 MOPS 靜態 HTML 備援...")

    class RevParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_td = False; self.cur = []; self.row = []; self.cells = []
        def handle_starttag(self, tag, attrs):
            if tag == "tr":  self.row = []; self.cur = []
            if tag == "td":  self.in_td = True; self.cur = []
        def handle_endtag(self, tag):
            if tag == "td":
                self.in_td = False
                self.row.append(" ".join(self.cur).strip())
            if tag == "tr" and len(self.row) >= 7:
                self.cells.append(self.row[:])
        def handle_data(self, data):
            if self.in_td: self.cur.append(data.strip())

    for typek in ["sii", "otc"]:
        try:
            url = (f"https://mops.twse.com.tw/nas/t21/{typek}/"
                   f"t21sc03_{year_roc}_{month}_0.html")
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            # 嘗試自動偵測或 big5
            try:
                text = res.content.decode("utf-8")
            except UnicodeDecodeError:
                text = res.content.decode("big5", errors="replace")
            parser = RevParser()
            parser.feed(text)
            ok = 0
            for row in parser.cells:
                try:
                    code = row[0].strip()
                    if not (code.isdigit() and len(code) == 4): continue
                    yoy_str = row[6].replace(",", "").replace("+", "").strip()
                    result[code] = round(float(yoy_str), 1)
                    ok += 1
                except Exception: continue
            print(f"  [月營收] MOPS {typek} {year_roc}/{month} → {ok} 檔")
        except Exception as e:
            print(f"  [月營收] MOPS {typek} 失敗：{e}")
    return result


def fetch_twse_industry():
    """用靜態對照表把 Yahoo 英文產業轉中文"""
    fetch_all_industries(list(_name_cache.keys()))

def fetch_lending_one_day(date_str):
    """抓單一日期的借券資料"""
    try:
        url = f"https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?date={date_str}&response=json"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        d = res.json()
        if d.get("stat") != "OK":
            return None
        result = {}
        for r in d.get("data", []):
            try:
                code = r[0].strip()
                if not (code.isdigit() and len(code) == 4):
                    continue
                sell = round(int(r[9].replace(",","") or 0) / 1000)
                bal  = round(int(r[12].replace(",","") or 0) / 1000)
                result[code] = {"volume": sell, "balance": bal}
            except Exception:
                continue
        return result if result else None
    except Exception:
        return None


def fetch_lending_history():
    """往前抓最近5個交易日的借券資料，建立 _lending_history_cache"""
    global _lending_history_cache
    from datetime import datetime, timedelta
    today = datetime.now()
    collected = []
    days_back = 0
    while len(collected) < 5 and days_back < 15:
        days_back += 1
        d = today - timedelta(days=days_back)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y%m%d")
        data = fetch_lending_one_day(date_str)
        if data:
            collected.append((d.strftime("%Y-%m-%d"), data))
            print(f"    [借券歷史] {date_str} ✓ {len(data)}檔")
        time.sleep(0.5)
    _lending_history_cache = {}
    for date_str, day_data in reversed(collected):
        for sid, vals in day_data.items():
            if sid not in _lending_history_cache:
                _lending_history_cache[sid] = []
            _lending_history_cache[sid].append({
                "date": date_str,
                "volume": vals["volume"],
                "balance": vals["balance"]
            })
    print(f"  [借券歷史] 完成，共 {len(collected)} 天")


def fetch_twse_name_lending():
    """從 TWSE 一次抓全市場：融資融券+借券餘額（含股票名稱）"""
    global _name_cache, _lending_cache
    for days_back in (0, 1, 2, 3):
        try:
            url = f"https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?date={get_twse_date(days_back)}&response=json"
            res = requests.get(url, headers=HEADERS, timeout=15)
            d = res.json()
            if d.get("stat") != "OK" or not d.get("data"):
                print(f"  [TWSE借券] {get_twse_date(days_back)} 無資料，換前一日")
                continue
            fields = d.get("fields", [])
            rows   = d.get("data",   [])
            print(f"  [TWSE借券] 欄位({len(fields)}個): {fields}")
            print(f"  [TWSE借券] 第一筆資料: {rows[0] if rows else 'N/A'}")
            # TWT93U 固定欄位：
            # r[0]=代號, r[1]=名稱
            # r[8]=借券前日餘額, r[9]=借券當日賣出, r[10]=借券還券, r[12]=借券當日餘額
            i_code      = 0
            i_name      = 1
            i_lend_sell = 9   # 借券當日賣出（股）
            i_lend_ret  = 10  # 借券還券（股）
            i_lend_bal  = 12  # 借券當日餘額（股）
            cnt_name = cnt_lend = 0
            for r in rows:
                try:
                    code = r[i_code].strip() if i_code < len(r) else ""
                    if not (code.isdigit() and len(code) == 4):
                        continue
                    name = r[i_name].strip() if i_name < len(r) else ""
                    if name and code not in _name_cache:
                        _name_cache[code] = (name, "其他")
                        cnt_name += 1
                    if i_lend_bal >= 0 and i_lend_bal < len(r):
                        bal  = int(r[i_lend_bal].replace(",","") or 0)
                        sell = int(r[i_lend_sell].replace(",","") or 0) if i_lend_sell >= 0 and i_lend_sell < len(r) else 0
                        ret  = int(r[i_lend_ret].replace(",","")  or 0) if i_lend_ret  >= 0 and i_lend_ret  < len(r) else 0
                        # TWT93U 單位是股，÷1000 轉張
                        _lending_cache[code] = {
                            "volume":  round(sell / 1000),
                            "balance": round(bal  / 1000),
                            "return":  round(ret  / 1000)
                        }
                        cnt_lend += 1
                except Exception:
                    continue
            print(f"  [TWSE借券] 名稱{cnt_name}檔 借券{cnt_lend}檔")
            break  # 成功就跳出
        except Exception as e:
            print(f"  [TWSE借券] 失敗：{e}")
            break



# Yahoo Finance industry → 台灣中文產業對照表
INDUSTRY_MAP = {
    # 半導體 / 電子
    "Semiconductors": "半導體",
    "Semiconductor Equipment & Materials": "半導體設備",
    "Semiconductor Memory": "半導體",
    "Electronic Components": "電子零組件",
    "Electronic Equipment & Instruments": "電子設備",
    "Electrical Components & Equipment": "電器電纜",
    "Computer Hardware": "電腦及週邊",
    "Consumer Electronics": "消費性電子",
    "Communication Equipment": "通信網路",
    "Telecom Services": "電信服務",
    "Software—Application": "軟體",
    "Software—Infrastructure": "軟體",
    "Information Technology Services": "資訊服務",
    "Scientific & Technical Instruments": "電子設備",
    # 電機機械
    "Electrical Equipment": "電機機械",
    "Specialty Industrial Machinery": "電機機械",
    "Industrial Machinery": "電機機械",
    "Farm & Heavy Construction Machinery": "電機機械",
    "Metal Fabrication": "電機機械",
    # 汽車
    "Auto Parts": "汽車零組件",
    "Auto Manufacturers": "汽車",
    # 航運 / 交通
    "Marine Shipping": "航運",
    "Airlines": "航空",
    "Airports & Air Services": "航空",
    "Integrated Freight & Logistics": "物流",
    "Trucking": "物流",
    "Railroads": "物流",
    # 原物料
    "Steel": "鋼鐵",
    "Aluminum": "鋼鐵",
    "Copper": "鋼鐵",
    "Other Industrial Metals & Mining": "鋼鐵",
    "Gold": "金屬礦業",
    "Silver": "金屬礦業",
    # 化學
    "Chemicals": "化學工業",
    "Specialty Chemicals": "化學工業",
    "Agricultural Inputs": "化學工業",
    "Rubber & Plastics": "橡膠塑膠",
    # 紡織
    "Textile Manufacturing": "紡織",
    "Apparel Manufacturing": "紡織",
    "Footwear & Accessories": "紡織",
    # 建材 / 房地產
    "Building Materials": "建材",
    "Construction Materials": "建材",
    "Engineering & Construction": "建材營造",
    "Real Estate—Development": "建材營造",
    "Real Estate Services": "建材營造",
    "REIT—Industrial": "建材營造",
    "REIT—Retail": "建材營造",
    # 紙
    "Paper & Paper Products": "造紙",
    "Packaging & Containers": "造紙",
    # 水泥
    "Cement": "水泥",
    "Cement & Aggregates": "水泥",
    # 能源
    "Oil & Gas Integrated": "油電燃氣",
    "Oil & Gas Refining & Marketing": "油電燃氣",
    "Utilities—Regulated Gas": "油電燃氣",
    "Utilities - Regulated Gas": "油電燃氣",
    "Utilities—Regulated Water": "公用事業",
    "Oil & Gas Exploration & Production": "油電燃氣",
    "Oil & Gas Equipment & Services": "油電燃氣",
    # 公用事業 / 綠能
    "Utilities—Regulated Electric": "公用事業",
    "Utilities—Renewable": "綠能",
    "Solar": "太陽能",
    # 金融
    "Banks—Regional": "銀行",
    "Banks - Regional": "銀行",
    "Banks—Diversified": "銀行",
    "Financial Conglomerates": "金融控股",
    "Insurance—Life": "壽險",
    "Insurance—Property & Casualty": "保險",
    "Insurance Brokers": "保險",
    "Capital Markets": "證券",
    "Asset Management": "投信投顧",
    "Credit Services": "金融",
    # 生技
    "Biotechnology": "生技",
    "Drug Manufacturers": "生技",
    "Medical Devices": "醫療器材",
    "Medical Instruments & Supplies": "醫療器材",
    "Diagnostics & Research": "生技",
    # 零售
    "Specialty Retail": "零售百貨",
    "Department Stores": "百貨",
    "Discount Stores": "零售",
    "Internet Retail": "電商",
    # 觀光
    "Restaurants": "餐飲",
    "Lodging": "觀光飯店",
    "Travel Services": "觀光",
    "Leisure": "觀光",
    # 食品
    "Packaged Foods": "食品",
    "Beverages—Non-Alcoholic": "食品",
    "Beverages—Brewers": "食品",
    "Farm Products": "食品",
    "Confectioners": "食品",
    # 媒體
    "Broadcasting": "媒體",
    "Entertainment": "娛樂",
    "Publishing": "媒體",
    "Advertising Agencies": "媒體",
    # 其他
    "Conglomerates": "其他",
    "Business Services": "其他",
    "Staffing & Employment Services": "其他",
    "Security & Protection Services": "其他",
    "Waste Management": "其他",
    # 連字號變體（Yahoo Finance 有時用 " - " 取代 "—"）
    "Insurance - Life":                "壽險",
    "Insurance - Property & Casualty": "保險",
    "Banks - Diversified":             "銀行",
    "Banks - Regional":                "銀行",
    # 其他缺漏
    "Electronics & Computer Hardware": "電腦及週邊",
    "Tools & Accessories":             "其他",
    "Personal Products":               "其他",
    "Household & Personal Products":   "其他",
    "Health Information Services":     "生技",
    "Electronic Gaming & Multimedia":  "其他電子",
    "Internet Content & Information":  "其他",
    "Diversified Industrials":         "電機機械",
    "Specialty Business Services":     "其他",
    "Gambling":                        "觀光",
    "Rental & Leasing Services":       "其他",
    "Shell Companies":                 "其他",
}

# 中文產業 → sector_rotation key（供前端查詢輪動階段用）
SECTOR_KEY_MAP = {
    "半導體":    "半導體", "半導體設備":  "半導體",
    "電子工業":  "電子工業", "消費性電子": "電子工業", "電子設備": "電子工業",
    "電腦及週邊":"電腦週邊", "電腦週邊":   "電腦週邊",
    "通信網路":  "通信網路", "電信服務":   "通信網路",
    "電子零組件":"電子零組件",
    "電子通路":  "電子通路",
    "資訊服務":  "資訊服務", "軟體": "資訊服務", "數位雲端": "數位雲端",
    "其他電子":  "其他電子",
    "金融保險":  "金融保險", "金融控股": "金融保險", "銀行": "金融保險",
    "壽險": "金融保險", "保險": "金融保險", "證券": "金融保險", "金融": "金融保險",
    "建材營造":  "建材營造", "建材": "建材營造",
    "航運":      "航運",     "航空": "航運",
    "觀光餐旅":  "觀光餐旅", "觀光": "觀光餐旅", "觀光飯店": "觀光餐旅", "餐飲": "觀光餐旅",
    "貿易百貨":  "貿易百貨", "零售百貨": "貿易百貨", "百貨": "貿易百貨", "零售": "貿易百貨",
    "油電燃氣":  "油電燃氣",
    "綠能環保":  "綠能環保", "太陽能": "綠能環保", "綠能": "綠能環保",
    "電機機械":  "電機機械",
    "生技醫療":  "生技醫療", "生技": "生技醫療", "醫療器材": "生技醫療",
    "鋼鐵":      "鋼鐵",
    "汽車":      "汽車",     "汽車零組件": "汽車",
    "食品":      "食品",
    "紡織":      "紡織",
    "水泥":      "水泥",
    "塑膠":      "塑膠",     "橡膠塑膠": "橡膠",
    "電器電纜":  "電器電纜",
    "化學工業":  "化學",     "化學": "化學", "化學生技": "化學生技",
    "玻璃陶瓷":  "玻璃陶瓷",
    "造紙":      "造紙",
    "橡膠":      "橡膠",
    "運動休閒":  "運動休閒",
    "居家生活":  "居家生活",
    "其他":      "其他",
}

TW_INDUSTRIES = list(set(INDUSTRY_MAP.values()))

# 股票代號 → sector_rotation key 靜態對照表（比 yfinance industry 更可靠）
# 覆蓋 yfinance 的錯誤分類（台灣大→通信網路、山隆→航運、中纖→紡織 等）
STOCK_SECTOR_MAP = {
    "1101": "建材營造",  # 台泥
    "1213": "食品",      # 大飲
    "1235": "食品",      # 興泰
    "1301": "化學",      # 台塑
    "1303": "化學",      # 南亞
    "1304": "化學",      # 台聚
    "1305": "化學",      # 華夏
    "1307": "紡織",      # 三芳
    "1308": "化學",      # 亞聚
    "1309": "化學",      # 台達化
    "1310": "化學",      # 台苯
    "1312": "化學",      # 國喬
    "1313": "化學",      # 聯成
    "1314": "化學",      # 中石化
    "1326": "化學",      # 台化
    "1402": "紡織",      # 遠東新
    "1466": "紡織",      # 聚隆
    "1536": "汽車",      # 和大
    "1589": "電機機械",  # 永冠-KY
    "1597": "電機機械",  # 直得
    "1605": "鋼鐵",      # 華新
    "1708": "化學",      # 東鹼
    "1710": "化學",      # 東聯
    "1711": "化學",      # 永光
    "1717": "化學",      # 長興
    "1718": "紡織",      # 中纖（yfinance 誤標為銀行）
    "1723": "化學",      # 中碳
    "1732": "居家生活",  # 毛寶
    "1736": "運動休閒",  # 喬山（健身器材，yfinance 誤標為觀光）
    "1760": "生技醫療",  # 寶齡富錦
    "1802": "建材營造",  # 台玻
    "1904": "造紙",      # 正隆
    "1905": "造紙",      # 華紙
    "2002": "鋼鐵",      # 中鋼
    "2013": "鋼鐵",      # 中鋼構
    "2014": "鋼鐵",      # 中鴻
    "2027": "鋼鐵",      # 大成鋼
    "2032": "鋼鐵",      # 新鋼
    "2038": "鋼鐵",      # 海光
    "2059": "居家生活",  # 川湖
    "2207": "汽車",      # 和泰車
    "2303": "半導體",    # 聯電
    "2308": "電子零組件",# 台達電
    "2313": "電子零組件",# 華通
    "2317": "電子零組件",# 鴻海
    "2323": "電腦週邊",  # 中環
    "2324": "電腦週邊",  # 仁寶
    "2327": "電子零組件",# 國巨
    "2329": "半導體",    # 華泰
    "2330": "半導體",    # 台積電
    "2337": "半導體",    # 旺宏
    "2344": "半導體",    # 華邦電
    "2349": "電腦週邊",  # 錸德
    "2351": "半導體",    # 順德
    "2353": "電腦週邊",  # 宏碁
    "2357": "電腦週邊",  # 華碩
    "2367": "電子零組件",# 燿華
    "2369": "半導體",    # 菱生
    "2371": "電機機械",  # 大同（yfinance 誤標為其他）
    "2382": "電腦週邊",  # 廣達
    "2388": "半導體",    # 威盛
    "2395": "電腦週邊",  # 研華
    "2406": "綠能環保",  # 國碩
    "2408": "半導體",    # 南亞科
    "2409": "電子零組件",# 友達
    "2412": "通信網路",  # 中華電
    "2424": "通信網路",  # 隴華（與兆赫同類）
    "2425": "電腦週邊",  # 承啟
    "2431": "電子零組件",# 聯昌
    "2449": "半導體",    # 京元電子
    "2454": "半導體",    # 聯發科
    "2455": "半導體",    # 全新
    "2474": "電機機械",  # 可成
    "2485": "通信網路",  # 兆赫
    "2489": "電機機械",  # 瑞軒
    "2603": "航運",      # 長榮
    "2605": "航運",      # 新興
    "2609": "航運",      # 陽明
    "2610": "航運",      # 華航
    "2615": "航運",      # 萬海
    "2616": "航運",      # 山隆（yfinance 誤標為零售百貨）
    "2618": "航運",      # 長榮航
    "2801": "金融保險",  # 彰銀
    "2834": "金融保險",  # 臺企銀
    "2867": "金融保險",  # 三商壽
    "2880": "金融保險",  # 華南金
    "2881": "金融保險",  # 富邦金
    "2882": "金融保險",  # 國泰金
    "2883": "金融保險",  # 凱基金
    "2884": "金融保險",  # 玉山金
    "2885": "金融保險",  # 元大金
    "2886": "金融保險",  # 兆豐金
    "2887": "金融保險",  # 台新新光金
    "2890": "金融保險",  # 永豐金
    "2891": "金融保險",  # 中信金
    "2892": "金融保險",  # 第一金
    "3008": "電子零組件",# 大立光
    "3026": "電子零組件",# 禾伸堂
    "3034": "半導體",    # 聯詠
    "3041": "半導體",    # 揚智
    "3045": "通信網路",  # 台灣大（yfinance 誤標為零售百貨）
    "3049": "電子零組件",# 精金
    "3167": "電機機械",  # 大量
    "3231": "電腦週邊",  # 緯創
    "3450": "半導體",    # 聯鈞
    "3481": "電子零組件",# 群創
    "3530": "半導體",    # 晶相光
    "3576": "綠能環保",  # 聯合再生
    "3583": "半導體",    # 辛耘
    "3653": "電子零組件",# 健策
    "3702": "電子通路",  # 大聯大
    "3711": "半導體",    # 日月光投控
    "3715": "電子零組件",# 定穎投控
    "3717": "電子零組件",# 聯嘉投控
    "4148": "化學",      # 全宇生技-KY（化學原料）
    "4526": "電機機械",  # 東台
    "4551": "汽車",      # 智伸科
    "4566": "電機機械",  # 時碩工業
    "4576": "電機機械",  # 大銀微系統
    "4739": "化學",      # 康普
    "4746": "生技醫療",  # 台耀
    "4755": "化學",      # 三福化
    "4766": "化學",      # 南寶
    "4906": "通信網路",  # 正文
    "4919": "半導體",    # 新唐
    "4927": "電子零組件",# 泰鼎-KY
    "4938": "電腦週邊",  # 和碩
    "4956": "電子零組件",# 光鋐
    "4958": "電子零組件",# 臻鼎-KY
    "4967": "電腦週邊",  # 十銓
    "4977": "電腦週邊",  # 眾達-KY
    "4989": "鋼鐵",      # 榮科
    "5521": "建材營造",  # 工信
    "5871": "金融保險",  # 中租-KY
    "5880": "金融保險",  # 合庫金
    "5906": "紡織",      # 台南-KY
    "6116": "電子零組件",# 彩晶
    "6155": "電子零組件",# 鈞寶
    "6209": "電子零組件",# 今國光
    "6225": "電子工業",  # 天瀚
    "6226": "電子零組件",# 光鼎
    "6269": "電子零組件",# 台郡
    "6282": "電機機械",  # 康舒
    "6415": "半導體",    # 矽力-KY
    "6438": "電機機械",  # 迅得
    "6443": "綠能環保",  # 元晶
    "6451": "半導體",    # 訊芯-KY
    "6505": "油電燃氣",  # 台塑化
    "6550": "生技醫療",  # 北極星藥業-KY
    "6585": "化學",      # 鼎基
    "6669": "電腦週邊",  # 緯穎
    "6672": "電子零組件",# 騰輝電子-KY
    "6689": "數位雲端",  # 伊雲谷
    "6722": "汽車",      # 輝創
    "6770": "半導體",    # 力積電
    "6789": "半導體",    # 采鈺
    "6830": "半導體",    # 汎銓
    "6890": "紡織",      # 來億-KY
    "6919": "生技醫療",  # 康霈
    "7610": "鋼鐵",      # 聯友金屬-創
    "7711": "電腦週邊",  # 永擎
    "7722": "資訊服務",  # LINEPAY
    "7730": "電機機械",  # 暉盛-創
    "7750": "電機機械",  # 新代
    "7780": "食品",      # 大研生醫（保健食品）
    "8021": "電機機械",  # 尖點
    "8046": "電子零組件",# 南電
    "8110": "半導體",    # 華東
    "8112": "半導體",    # 至上
    "8215": "電子零組件",# 明基材
    "8422": "綠能環保",  # 可寧衛（廢棄物處理，yfinance 誤標為其他）
    "8940": "觀光餐旅",  # 新天地
    "9919": "居家生活",  # 康那香
    "9929": "其他",      # 秋雨（待確認）
}

def fetch_all_industries(sids):
    """用靜態對照表把 Yahoo 英文產業轉成中文，不消耗任何 API quota"""
    ok = 0
    for sid in sids:
        name, ind = _name_cache.get(sid, (sid, "其他"))
        if ind and ind != "其他" and any('一' <= ch <= '鿿' for ch in ind):
            continue  # 已有中文產業
        # 查對照表
        zh = INDUSTRY_MAP.get(ind, "")
        if not zh:
            # 模糊比對
            for en, tw in INDUSTRY_MAP.items():
                if en.lower() in (ind or "").lower():
                    zh = tw
                    break
        if zh:
            _name_cache[sid] = (name, zh)
            ok += 1
    print(f"  [產業] 靜態對照轉換 {ok}/{len(sids)} 檔")


def fetch_stock_name_industry(sid):
    """從快取取中文名稱和產業"""
    return _name_cache.get(sid, (sid, "其他"))


def get_lending_from_cache(sid):
    """從 TWSE 快取取借券資料"""
    d = _lending_cache.get(sid)
    if not d or (d["volume"] == 0 and d["balance"] == 0):
        return None
    # 估算金額（無收盤價，暫用 0）
    return {
        "volume":  d["volume"],
        "balance": d["balance"],
        "amount":  0,
        "daily":   [{"date": get_date(), "volume": d["volume"], "amount": 0}]
    }


def finmind(dataset, stock_id, start_date, retry=2):
    params = {
        "dataset":    dataset,
        "data_id":    stock_id,
        "start_date": start_date,
        "end_date":   get_date(),
        "token":      FINMIND_TOKEN,
    }
    for attempt in range(retry + 1):
        try:
            res = requests.get(FINMIND_URL, params=params, timeout=25)
            d = res.json()
            status = d.get("status")
            if status == 200:
                return d.get("data", [])
            if status == 402:
                # 達到上限，等待後重試
                wait = 90 * (attempt + 1)
                print(f"    [FM] 達到上限，等待 {wait}s 後重試...")
                time.sleep(wait)
                continue
            print(f"    [FM] {dataset} {stock_id}: {d.get('msg','')}")
            return []
        except Exception as e:
            print(f"    [FM] {dataset} {stock_id} 例外：{e}")
            return []
    return []

def fetch_chips(sid):
    rows = finmind("TaiwanStockInstitutionalInvestorsBuySell", sid, get_date(30))
    if not rows:
        return {}

    rows.sort(key=lambda x: x.get("date",""))
    latest_date = rows[-1].get("date","")
    latest = [r for r in rows if r.get("date") == latest_date]

    # Debug：印出第一筆的欄位名稱，方便確認
    if latest:
        print(f"      [籌碼] 最新日期={latest_date} 共{len(latest)}筆")
        print(f"        欄位: {list(latest[0].keys())}")
        for r in latest[:3]:
            name=r.get('name','?'); buy=r.get('buy',0); sell=r.get('sell',0)
            print(f"        {name}: buy={buy} sell={sell} net={(int(buy or 0)-int(sell or 0))//1000}張")
    else:
        print(f"      [籌碼] 無資料！rows總數={len(rows)}")

    def is_foreign(name):
        n = name.lower()
        # Foreign_Investor = 外資，Foreign_Dealer_Self = 外資自營，排除
        return ("外資" in name or n == "foreign_investor") and "dealer" not in n
    def is_trust(name):
        n = name.lower()
        return "投信" in name or n == "investment_trust"
    def is_dealer(name):
        n = name.lower()
        # Dealer_self = 自營商自行買賣, Dealer_Hedging = 自營商避險
        return "自營" in name or n in ("dealer_self", "dealer_hedging", "dealer")

    foreign = trust = dealer = 0
    for r in latest:
        name = r.get("name","")
        buy  = int(r.get("buy",  r.get("Buy",  0)) or 0)
        sell = int(r.get("sell", r.get("Sell", 0)) or 0)
        net  = (buy - sell) // 1000  # 單位：股 → 張
        if is_foreign(name): foreign += net
        elif is_trust(name): trust   += net
        elif is_dealer(name):dealer  += net

    # 連續天數
    def calc_con(name_key):
        days = sorted({r["date"] for r in rows}, reverse=True)
        con = 0
        for d in days:
            day_rows = [r for r in rows if r["date"] == d]
            net = 0
            for r in day_rows:
                n = r.get("name","")
                v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 1000
                nl = n.lower()
                if name_key == "foreign" and ("外資" in n or nl == "foreign_investor") and "dealer" not in nl: net += v
                elif name_key == "trust"   and ("投信" in n or nl == "investment_trust"): net += v
                elif name_key == "dealer"  and ("自營" in n or nl in ("dealer_self","dealer_hedging","dealer")): net += v
            if con == 0:
                con = 1 if net > 0 else -1 if net < 0 else 0
            elif (con > 0 and net > 0): con += 1
            elif (con < 0 and net < 0): con -= 1
            else: break
        return con

    f_con = calc_con("foreign")
    t_con = calc_con("trust")
    d_con = calc_con("dealer")

    # 30日各法人累計
    date_30 = get_date(30)
    rows30 = [r for r in rows if r.get("date","") >= date_30]
    f30 = t30 = d30 = 0
    for r in rows30:
        n = r.get("name",""); net = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 1000
        nl = n.lower()
        if ("外資" in n or nl == "foreign_investor") and "dealer" not in nl: f30 += net
        elif "投信" in n or nl == "investment_trust": t30 += net
        elif "自營" in n or nl in ("dealer_self","dealer_hedging","dealer"): d30 += net

    # 本波段連續累計
    def con_sum(name_key, con_days):
        n = max(abs(con_days), 1)  # 至少取1天（連續0日也顯示今日值）
        total = 0
        days = sorted({r["date"] for r in rows}, reverse=True)[:n]
        for d in days:
            for r in rows:
                if r["date"] != d: continue
                nm = r.get("name",""); v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 1000
                nml = nm.lower()
                if name_key == "foreign" and ("外資" in nm or nml == "foreign_investor") and "dealer" not in nml: total += v
                elif name_key == "trust"  and ("投信" in nm or nml == "investment_trust"): total += v
                elif name_key == "dealer" and ("自營" in nm or nml in ("dealer_self","dealer_hedging","dealer")): total += v
        return total

    f_con_sum = con_sum("foreign", f_con)
    t_con_sum = con_sum("trust",   t_con)
    d_con_sum = con_sum("dealer",  d_con)

    # 近5日明細
    days5 = sorted({r["date"] for r in rows}, reverse=True)[:5]
    daily = []
    for d in days5:
        df = dt = dd = 0
        for r in rows:
            if r["date"] != d: continue
            n = r.get("name",""); v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 1000
            nl2 = n.lower()
            if ("外資" in n or nl2 == "foreign_investor") and "dealer" not in nl2: df += v
            elif "投信" in n or nl2 == "investment_trust": dt += v
            elif "自營" in n or nl2 in ("dealer_self","dealer_hedging","dealer"): dd += v
        daily.append({"date": d, "foreign": df, "trust": dt, "dealer": dd, "total": df+dt+dd})

    return {
        "foreign": foreign, "trust": trust, "dealer": dealer,
        "foreign_con": f_con, "trust_con": t_con, "dealer_con": d_con,
        "foreign_con_sum": f_con_sum, "trust_con_sum": t_con_sum, "dealer_con_sum": d_con_sum,
        "foreign_sum30": f30, "trust_sum30": t30, "dealer_sum30": d30,
        "daily": daily,
    }


# ── 4. Gemini 新聞 ────────────────────────────────────────────────

THEMES = ["AI伺服器","矽光子","低軌衛星","散熱","CoWoS先進封裝","HBM","車用電子",
          "電動車","儲能","太陽能","5G","網通","生技新藥","醫材","金融科技",
          "航運","鋼鐵原料","半導體設備","ASIC設計","記憶體","面板",
          "消費電子","工業電腦","機器人","軍工","其他"]

# 靜態概念對照表 {代號: [概念1, 概念2, ...]}
STOCK_THEMES_MAP = {
    # 矽智財（IP）
    "3661": ["矽智財"],
    "3443": ["矽智財"],
    "6643": ["矽智財"],
    "3035": ["矽智財"],
    # 先進製程晶圓代工
    "2330": ["先進製程", "CoWoS先進封裝", "AI伺服器"],
    # 封測測試
    "3711": ["封測"],
    # 網通
    "2454": ["網通", "AI伺服器"],
    "2345": ["網通"],
    # 伺服器管理晶片（BMC）
    "5274": ["伺服器管理晶片"],
    "4919": ["伺服器管理晶片"],
    # 高速傳輸IC
    "5269": ["高速傳輸IC"],
    "4966": ["高速傳輸IC"],
    # 電源供應器
    "2308": ["電源供應器", "散熱", "自動化"],
    "2301": ["電源供應器"],
    "2385": ["電源供應器"],
    # 散熱
    "3017": ["散熱"],
    "3324": ["散熱"],
    "6230": ["散熱"],
    "2421": ["散熱"],
    # 伺服器電路板（PCB）
    "2368": ["PCB"],
    "8155": ["PCB"],
    "3044": ["PCB"],
    # 銅箔基板（CCL）
    "6274": ["銅箔基板"],
    "2383": ["銅箔基板"],
    "6213": ["銅箔基板", "CoWoS先進封裝"],
    # 載板（ABF）
    "3037": ["載板", "CoWoS先進封裝"],
    "8046": ["載板", "CoWoS先進封裝"],
    "3189": ["載板", "CoWoS先進封裝"],
    # 伺服器機殼
    "3013": ["伺服器機殼"],
    "8210": ["伺服器機殼"],
    "3032": ["伺服器機殼"],
    "3693": ["伺服器機殼"],
    # 連接器
    "3533": ["連接器"],
    # 伺服器代工廠
    "2317": ["AI伺服器"],
    "2382": ["AI伺服器"],
    "6669": ["AI伺服器"],
    "2356": ["AI伺服器"],
    # 精密機械
    "2049": ["精密機械"],
    "4583": ["精密機械"],
    "1597": ["精密機械"],
    # 自動化/系統整合
    "2464": ["自動化"],
    "6215": ["自動化"],
    # AI視覺
    "2359": ["AI視覺"],
    "6414": ["AI視覺", "工業電腦"],
    "3059": ["AI視覺"],
    # AI應用
    "2395": ["AI應用", "工業電腦"],
    # 記憶體（原廠）
    "2408": ["記憶體", "HBM"],
    "2344": ["記憶體"],
    "3006": ["記憶體"],
    # 記憶體（封裝）
    "3260": ["記憶體封裝"],
    "4967": ["記憶體封裝"],
    "8271": ["記憶體封裝"],
    "2451": ["記憶體封裝"],
    "5289": ["記憶體封裝"],
    # 矽光子/CPO
    "3081": ["矽光子", "CPO"],
    "3163": ["矽光子", "CPO"],
    # 重電族群
    "1519": ["重電", "AI基礎建設"],
    "1503": ["重電", "AI基礎建設"],
    "3680": ["重電"],
    "3131": ["重電"],
    "6187": ["重電"],
    "3413": ["重電"],
    # 摺疊機軸承
    "6805": ["摺疊機", "精密機械"],
    "3548": ["摺疊機"],
    # 玻璃基板
    "1802": ["玻璃基板"],
    "3481": ["玻璃基板"],
}

def fetch_news(name, sid):
    if not GEMINI_API_KEY:
        return [], []
    prompt = f"""請分析台股「{name}（{sid}）」的最新市場資訊。
回傳 JSON（只回JSON，不要markdown）：
{{
  "themes": ["最多3個概念題材，從清單選：{','.join(THEMES)}"],
  "news": [
    {{"title":"新聞標題","source":"來源","tag":"標籤","sentiment":"bullish/bearish/neutral","url":"#"}}
  ]
}}
新聞最多3則，sentiment 必須是 bullish/bearish/neutral 之一。"""
    safety = [
        {"category":"HARM_CATEGORY_HARASSMENT",       "threshold":"BLOCK_NONE"},
        {"category":"HARM_CATEGORY_HATE_SPEECH",       "threshold":"BLOCK_NONE"},
        {"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold":"BLOCK_NONE"},
        {"category":"HARM_CATEGORY_DANGEROUS_CONTENT", "threshold":"BLOCK_NONE"},
    ]
    models = ["gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-2.0-flash"]
    for model in models:
        try:
            time.sleep(4)  # 避免超過每分鐘限制
            res = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                json={"contents":[{"parts":[{"text":prompt}]}],
                      "generationConfig":{"temperature":0.3,"maxOutputTokens":600},
                      "safetySettings": safety},
                headers={"Content-Type":"application/json"}, timeout=20
            )
            rj = res.json()
            if "error" in rj:
                msg = rj['error'].get('message','')[:80]
                print(f"    [Gemini] {sid} {model} 錯誤: {msg}")
                if "quota" in msg.lower() or "exceeded" in msg.lower():
                    time.sleep(15)  # quota 超限等15秒再換模型
                continue
            text = rj["candidates"][0]["content"]["parts"][0]["text"]
            text = text.replace("```json","").replace("```","").strip()
            d = json.loads(text)
            print(f"    [Gemini] {sid} {model} ✓")
            return d.get("news", []), d.get("themes", [])
        except Exception as e:
            print(f"    [Gemini] {sid} {model} 失敗：{e}")
            continue
    return [], []

# ── 5. 歷史回測工具函式 ───────────────────────────────────────────

_SIGNAL_LABELS_BT = {
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


def _bt_yahoo_snapshot(closes, highs, lows, volumes, i):
    """計算歷史第 i 日的技術指標快照"""
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


def bt_backtest_one_stock(closes, highs, lows, volumes, opens=None, stock_phase="RANGE"):
    """對單支股票的歷史資料逐日跑訊號偵測，回傳各訊號的結果清單"""
    n = len(closes)
    if n < 77:
        return []
    results = []
    for i in range(62, n - 15):
        snapshot = _bt_yahoo_snapshot(closes, highs, lows, volumes, i)
        if not snapshot:
            continue
        for sig in calc_signals(snapshot, {}, 50, stock_phase=stock_phase):
            entry  = sig["entry"]
            stop   = sig["stop_loss"]
            target = sig["target"]
            if target <= entry or entry <= stop:
                continue
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
            day5_close = closes[min(i + 5, n - 1)]
            import math
            if math.isnan(day5_close) or day5_close <= 0:
                continue
            # gain_pct：win/loss 用確定的出場價計算，inconclusive 用第5日收盤
            if outcome == "win":
                gain_pct = round((target - entry) / entry * 100, 2)
            elif outcome == "loss":
                gain_pct = round((stop   - entry) / entry * 100, 2)
            else:
                gain_pct = round((day5_close - entry) / entry * 100, 2)
            results.append({
                "type":     sig["type"],
                "outcome":  outcome,
                "gain_pct": gain_pct,
            })
    return results


def bt_aggregate_stats(all_results):
    """彙整回測結果，回傳各訊號類型的勝率統計"""
    from collections import defaultdict
    buckets = defaultdict(lambda: {
        "wins": 0, "losses": 0, "inconclusive": 0,
        "gain_sum": 0.0, "loss_sum": 0.0,
    })
    for r in all_results:
        t = r["type"]
        if r["outcome"] == "win":
            buckets[t]["wins"]     += 1
            buckets[t]["gain_sum"] += r["gain_pct"]
        elif r["outcome"] == "loss":
            buckets[t]["losses"]   += 1
            buckets[t]["loss_sum"] += r["gain_pct"]
        else:
            buckets[t]["inconclusive"] += 1
    stats = {}
    for t, b in buckets.items():
        decided = b["wins"] + b["losses"]
        total   = decided + b["inconclusive"]
        if total < 5:
            continue
        stats[t] = {
            "label":             _SIGNAL_LABELS_BT.get(t, t),
            "count":             total,
            "win_rate":          round(b["wins"] / decided, 3) if decided > 0 else 0.5,
            "avg_gain_pct":      round(b["gain_sum"] / b["wins"],   2) if b["wins"]   > 0 else 0.0,
            "avg_loss_pct":      round(b["loss_sum"] / b["losses"], 2) if b["losses"] > 0 else 0.0,
            "inconclusive_rate": round(b["inconclusive"] / total,   3) if total > 0 else 0.0,
        }
    return stats


def bt_update_tracking(prev_tracking, today_price_map, today_results, today_str, sector_rotation=None, today_high_map=None, today_low_map=None, today_open_map=None):
    """更新追蹤清單：更新舊記錄狀態，加入今日新訊號，保留最近 60 筆"""
    import copy
    updated = []
    for rec in prev_tracking:
        rec = copy.copy(rec)
        if rec.get("status") != "open":
            updated.append(rec); continue
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
                rec["status"]   = "win";  rec["resolved_date"] = today_str
                rec["gain_pct"] = round((rec["target"]    - rec["entry"]) / rec["entry"] * 100, 2)
            else:
                rec["status"]   = "loss"; rec["resolved_date"] = today_str
                rec["gain_pct"] = round((rec["stop_loss"] - rec["entry"]) / rec["entry"] * 100, 2)
        elif hit_target:
            rec["status"]   = "win";     rec["resolved_date"] = today_str
            rec["gain_pct"] = round((rec["target"]    - rec["entry"]) / rec["entry"] * 100, 2)
        elif hit_stop:
            rec["status"]   = "loss";    rec["resolved_date"] = today_str
            rec["gain_pct"] = round((rec["stop_loss"] - rec["entry"]) / rec["entry"] * 100, 2)
        elif days_held >= 20:
            rec["status"] = "expired"; rec["resolved_date"] = today_str
        updated.append(rec)
    # 加入今日新訊號（重複標注：同代號同類型已有 open 記錄則標記 repeat=True）
    open_keys = {(r["code"], r["type"]) for r in updated if r.get("status") == "open"}
    for stock in today_results:
        code = stock["code"]
        for sig in stock.get("signals", []):
            ep        = today_price_map.get(code, sig["entry"])
            is_repeat = (code, sig["type"]) in open_keys
            sk        = stock.get("sector_key", "")
            sdata     = (sector_rotation or {}).get(sk, {})
            updated.append({
                "code":          code,
                "name":          stock.get("name", code),
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
                "current_price": round(ep, 2),
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


# ── 5b. 警示訊號 ──────────────────────────────────────────────────

def calc_warnings(chips, yahoo, vol_month_ratio):
    warnings = []
    f_con = chips.get("foreign_con", 0)
    if f_con <= -5:  # 連賣5日以上
        warnings.append({"type":"chips_sell","level":"high","msg":f"外資連賣超{abs(f_con)}日"})

    price    = yahoo.get("price") or 0
    w52_high = yahoo.get("week52_high") or 0
    w52_low  = yahoo.get("week52_low") or 0
    if w52_high > w52_low and price > 0:
        pos = (price - w52_low) / (w52_high - w52_low)
        if pos > 0.9 and (vol_month_ratio or 1) < 0.8:
            warnings.append({"type":"high_price_low_vol","level":"mid",
                             "msg":f"近52週高點({int(pos*100)}%)且量能萎縮"})

    eg = yahoo.get("eps_growth") or 0
    if eg < -10:
        warnings.append({"type":"eps_decline","level":"mid",
                         "msg":f"EPS年增率{eg}%（衰退）"})
    return warnings


# ── 5b. 買點訊號偵測 ──────────────────────────────────────────────

_RR_MAP = {
    "BULL":          3.0,
    "BULL_PULLBACK": 2.0,
    "RANGE":         1.5,
    "BEAR_WEAK":     1.5,
    "BEAR_STRONG":   1.0,
}

_ALLOWED_SIGNALS = {
    "BULL":          {"breakout", "false_breakdown", "ma_pullback", "high_base", "retest", "ma60_support", "trend_cont"},
    "BULL_PULLBACK": {"ma_pullback", "retest"},
    "RANGE":         {"ma_pullback", "retest", "ma60_support"},
    "BEAR_WEAK":     {"false_breakdown", "retest"},
    "BEAR_STRONG":   {"false_breakdown"},
}


def calc_signals(yahoo, chips, rs_pct=50, stock_phase="RANGE",
                 market_regime="range", composite_score=0, structure=""):
    """
    偵測 7 種技術面買點訊號，回傳 list of dict。
    每個訊號：{type, label, strength, entry, stop_loss, target, risk, rr, reason,
               atr_stop, confirmations}
    strength: 'strong' | 'medium' | 'weak'
    """
    signals = []

    # 最低分數門檻：綜合分 < 45 不產生任何訊號（避免在弱股上假訊號）
    if composite_score > 0 and composite_score < 45:
        return []

    price     = yahoo.get("price")     or 0
    high20    = yahoo.get("high20")
    low20     = yahoo.get("low20")
    prev_low20= yahoo.get("prev_low20")
    prev_close= yahoo.get("prev_close")
    ma5       = yahoo.get("ma5")
    ma10      = yahoo.get("ma10")
    ma20      = yahoo.get("ma20")
    ma60      = yahoo.get("ma60")
    vol_day     = yahoo.get("vol_day_ratio") or 1.0
    avwap_swing = yahoo.get("avwap_swing")
    avwap_vol   = yahoo.get("avwap_vol")
    avwap_short = yahoo.get("avwap_short")
    m_z_val      = yahoo.get("m_z")
    rs_trend_val = yahoo.get("rs_trend_stock")
    sector_rs    = yahoo.get("sector_rs")

    if not price:
        return []

    # AVWAP 狀態標記
    _trend_ok = avwap_swing is None or price >= avwap_swing
    _mm_ok    = avwap_vol   is None or price >= avwap_vol
    _short_ok = avwap_short is None or price >= avwap_short

    # BULL 型態動能額外條件（Layer 2 嚴格版）：
    # breakout/high_base 需要 M>1.2（RS 明顯高於10日均）且 rs_trend>0 且産業RS>0
    _bull_momentum = (stock_phase != "BULL") or (
        m_z_val is not None and m_z_val > 1.2 and
        rs_trend_val is not None and rs_trend_val > 0 and
        (sector_rs is None or sector_rs > 0)
    )

    # 大盤相位過濾：根據大盤狀態限制允許的訊號類型
    _market_bear = (market_regime == "bear")
    _base_allowed = _ALLOWED_SIGNALS.get(stock_phase, _ALLOWED_SIGNALS["RANGE"])
    _MARKET_ALLOWED = {
        "bull":          _base_allowed,
        "bull_pullback": {"ma_pullback", "retest", "false_breakdown", "ma60_support"},
        "range":         {"ma_pullback", "retest", "false_breakdown", "ma60_support"},
        "bear":          {"false_breakdown"},
    }.get(market_regime, _base_allowed)

    # 訊號確認數（0~6，越高越可信）
    _chips_score = chips.get("chips_score_val", 0) or 0
    _confirmations = sum([
        _chips_score > 60,                            # 籌碼分強
        rs_pct >= 70,                                 # RS 百分位高
        (yahoo.get("vol_day_ratio") or 1) > 1.3,     # 量比放大
        stock_phase == "BULL",                        # 個股多頭
        market_regime == "bull",                      # 大盤多頭
        bool(avwap_swing and avwap_vol and avwap_short  # 三條 AVWAP 全對齊
             and price >= avwap_swing and price >= avwap_vol and price >= avwap_short),
    ])

    def _sig(type_, label, strength, entry, stop, reason):
        # 大盤相位篩選（優先）
        if type_ not in _MARKET_ALLOWED:
            return None
        # 個股型態篩選
        if type_ not in _ALLOWED_SIGNALS.get(stock_phase, _ALLOWED_SIGNALS["RANGE"]):
            return None
        _strength = strength
        _reason   = reason
        # 大盤熊市：訊號強度上限為 weak
        if _market_bear:
            _strength = "weak"
        if not _trend_ok:
            _strength = {"strong": "medium", "medium": "weak"}.get(_strength, _strength)
            _reason   = reason + "；⚠️趨勢破 AVWAP"
        if _mm_ok and avwap_vol:
            _reason = _reason + "；主力未跑✓"
        risk = round(entry - stop, 2) if stop else 0
        if risk <= 0:
            return None
        # 動態 RR（型態決定基礎，AVWAP 位置微調）
        rr = _RR_MAP.get(stock_phase, 2.0)
        if avwap_swing and price >= avwap_swing:
            rr *= 1.2
        elif avwap_short and price < avwap_short:
            rr *= 0.7
        rr = round(rr, 2)
        target = round(entry + risk * rr, 2)
        # ATR 動態停損（2×ATR，提供波動性調整的替代停損）
        atr = yahoo.get("atr_14")
        atr_stop = round(entry - 2 * atr, 2) if atr else None
        # 觸發進場價：今日高點 × (1 + buffer)，隔日突破才確認進場
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
        _buf = _TRIGGER_BUFFER.get(type_, 0.003)
        trigger_price = round(_today_high * (1 + _buf), 2)
        return {
            "type":          type_,
            "label":         label,
            "strength":      _strength,
            "entry":         round(entry, 2),
            "trigger_price": trigger_price,
            "stop_loss":     round(stop,  2),
            "atr_stop":      atr_stop,
            "target":        target,
            "risk":          risk,
            "rr":            rr,
            "reason":        _reason,
            "confirmations": _confirmations,
        }

    # 1. 突破（Breakout）：收盤突破20日高 + 量比≥1.5 + RS百分位≥70 + 短線節奏健康 + BULL動能確認
    if high20 and price > high20 and vol_day >= 1.5 and rs_pct >= 70 and _short_ok and _bull_momentum:
        # stop：取 low20 與 avwap_swing*0.99 較高者（主力成本線為首選停損位）
        _stop_bk = max(
            low20 or price * 0.95,
            round(avwap_swing * 0.99, 2) if avwap_swing else 0,
        )
        s = _sig("breakout", "突破", "strong", price, _stop_bk,
                 f"收盤({price})突破20日高({high20})，量比{vol_day:.1f}x，RS百分位{rs_pct}")
        if s: signals.append(s)

    # 2. 假跌破（False Breakdown）：昨收 < prev_low20 且今收 > low20 + RS≥50
    if low20 and prev_close and prev_low20 and prev_close < prev_low20 and price > low20 and rs_pct >= 50:
        s = _sig("false_breakdown", "假跌破", "medium", price, low20 * 0.98,
                 f"昨收({prev_close})跌破前20日低，今收({price})強力收復，RS百分位{rs_pct}")
        if s: signals.append(s)

    # 3. 均線回測 A（起漲型）：RS 40~60 + RS剛翻正 + AVWAP趨勢線守住
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20 and price > 0 and _trend_ok:
        dist_ma20 = (price - ma20) / ma20
        if 0 <= dist_ma20 <= 0.03 and 40 <= rs_pct < 60 and rs_trend_val is not None and rs_trend_val > 0:
            s = _sig("ma_pullback", "均線回測(起漲型)", "weak", price, ma20,
                     f"均線多頭，RS百分位{rs_pct}(40~60)，RS斜率剛翻正，AVWAP趨勢線守住")
            if s: signals.append(s)

    # 3b. 均線回測 B（主升型）：RS ≥ 60 + AVWAP趨勢線守住
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20 and price > 0 and _trend_ok:
        dist_ma20 = (price - ma20) / ma20
        if 0 <= dist_ma20 <= 0.03 and rs_pct >= 60:
            s = _sig("ma_pullback", "均線回測(主升型)", "medium", price, ma20,
                     f"均線多頭，RS百分位{rs_pct}(≥60)，AVWAP趨勢線守住")
            if s: signals.append(s)

    # 4. 強整再突（High Base Breakout）：收盤距20日高≤5% + 收盤>MA5 + RS百分位≥70 + 短線節奏健康 + BULL動能確認
    if high20 and ma5 and price > ma5 and rs_pct >= 70 and _short_ok and _bull_momentum:
        dist_high20 = (high20 - price) / high20
        if 0 <= dist_high20 <= 0.05:
            # stop：優先用 avwap_swing（主力成本線），其次 ma10
            _stop_hb = round(avwap_swing * 0.99, 2) if avwap_swing else (ma10 or price * 0.95)
            s = _sig("high_base", "強整再突", "medium", price, _stop_hb,
                     f"緊貼20日高({high20})整理，RS百分位{rs_pct}")
            if s: signals.append(s)

    # 5. 縮量回測 A（起漲型）：RS 40~60 + RS剛翻正 + AVWAP趨勢線守住 + 縮量
    if ma10 and ma20 and price > ma20 and _trend_ok:
        dist_ma10 = abs(price - ma10) / ma10
        if dist_ma10 <= 0.02 and vol_day < 1.0 and 40 <= rs_pct < 60 and rs_trend_val is not None and rs_trend_val > 0:
            s = _sig("retest", "縮量回測(起漲型)", "weak", price, ma20,
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})，RS百分位{rs_pct}(40~60)，RS斜率剛翻正")
            if s: signals.append(s)

    # 5b. 縮量回測 B（主升型）：RS ≥ 60 + AVWAP趨勢線守住 + 縮量
    if ma10 and ma20 and price > ma20 and _trend_ok:
        dist_ma10 = abs(price - ma10) / ma10
        if dist_ma10 <= 0.02 and vol_day < 1.0 and rs_pct >= 60:
            s = _sig("retest", "縮量回測(主升型)", "medium", price, ma20,
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})，RS百分位{rs_pct}(≥60)")
            if s: signals.append(s)

    # 6. MA60支撐（MA60 Support）：收盤距MA60在2%以內 + RS≥55 + AVWAP趨勢線守住
    if ma60 and rs_pct >= 55 and _trend_ok:
        dist_ma60 = (price - ma60) / ma60
        if 0 <= dist_ma60 <= 0.02:
            s = _sig("ma60_support", "MA60支撐", "weak", price, ma60 * 0.97,
                     f"收盤({price})貼近MA60({ma60})，RS百分位{rs_pct}")
            if s: signals.append(s)

    # 7. 趨勢延伸（Trend Continuation）：主升段 BULL 中繼確認
    # 適用於突破後進入主升段中段、不在任何其他觸發區的強勢股（如連續上漲但量能一般）
    _already_covered = any(sig.get("type") in ("breakout", "high_base") for sig in signals)
    if (not _already_covered
            and structure in ("主升段", "主升段✓", "主升段✓✓")
            and stock_phase == "BULL"
            and rs_pct >= 65
            and ma5 and ma10 and ma20
            and ma5 > ma10 > ma20                              # 全均線多頭排列
            and price > ma5                                    # 站穩 MA5 之上
            and high20 and price <= high20 * 1.15              # 距20日高不超過15%（避免追高）
            and (m_z_val is None or m_z_val > 1.0)            # 動能比值 > 1（RS仍高於10日均）
            and _trend_ok):                                    # AVWAP 趨勢線未跌破
        _stop_tc = max(
            round(ma20, 2),
            round(avwap_swing * 0.99, 2) if avwap_swing else 0,
        )
        _m_str = f"{m_z_val:.2f}" if m_z_val is not None else "N/A"
        s = _sig("trend_cont", "趨勢延伸", "medium", price, _stop_tc,
                 f"主升段均線多頭排列，RS百分位{rs_pct}，M={_m_str}，已站上所有均線✓")
        if s: signals.append(s)

    return signals


# ── 6. 評分（各項 0~100）─────────────────────────────────────────

def calc_score(chips, yahoo, vol_month_ratio, news_list, lending=None, rs_val=None, rs_pct=None, revenue_yoy=None):
    scores = {}
    price         = yahoo.get("price") or 0
    vol_today     = yahoo.get("volume") or 0
    shares        = yahoo.get("shares_outstanding") or 0

    # ── 籌碼 0~100（基礎分50，好加分差扣分）──────────────────
    chip_pts = 50  # 基礎分
    f_net = chips.get("foreign", 0)
    f_con = chips.get("foreign_con", 0)
    vol_lots = max(vol_today / 1000, 1)

    # 外資買超佔成交量%
    f_pct = f_net / vol_lots * 100
    if   f_pct > 10: chip_pts += 30
    elif f_pct >  5: chip_pts += 20
    elif f_pct >  2: chip_pts += 12
    elif f_pct >  0: chip_pts += 5
    elif f_pct < -5: chip_pts -= 15
    elif f_pct < -2: chip_pts -= 8

    # 外資連買天數
    if   f_con >= 10: chip_pts += 15
    elif f_con >=  5: chip_pts += 10
    elif f_con >=  3: chip_pts += 6
    elif f_con >=  1: chip_pts += 2
    elif f_con <= -5: chip_pts -= 12
    elif f_con <= -3: chip_pts -= 7
    elif f_con <= -1: chip_pts -= 3

    # 投信
    t_net = chips.get("trust", 0)
    t_con = chips.get("trust_con", 0)
    t_pct = t_net / vol_lots * 100
    if   t_pct >  3: chip_pts += 10
    elif t_pct >  1: chip_pts += 6
    elif t_pct >  0: chip_pts += 2
    elif t_pct < -1: chip_pts -= 5
    if   t_con >= 5: chip_pts += 5
    elif t_con >= 3: chip_pts += 3
    elif t_con >= 1: chip_pts += 1
    elif t_con <= -3: chip_pts -= 4

    # 自營商
    d_pct = chips.get("dealer", 0) / vol_lots * 100
    if   d_pct >  2: chip_pts += 3
    elif d_pct >  0: chip_pts += 1
    elif d_pct < -2: chip_pts -= 2

    # 借券：佔流通股比例
    lend_bal  = lending.get("balance", 0) if lending else 0
    lend_sell = lending.get("volume",  0) if lending else 0
    lend_hist = lending.get("daily",   []) if lending else []
    shares_lots = max(shares / 1000, 1)
    lend_pct = lend_bal / shares_lots * 100
    if   lend_pct >  8: chip_pts -= 12
    elif lend_pct >  5: chip_pts -= 8
    elif lend_pct >  3: chip_pts -= 4
    elif lend_bal == 0: chip_pts += 2

    # 借券趨勢
    if len(lend_hist) >= 3:
        bl = [h.get("balance", 0) for h in lend_hist[-3:]]
        if   bl[-1] > bl[0] * 1.2: chip_pts -= 4
        elif bl[-1] < bl[0] * 0.8: chip_pts += 3

    # 借券賣出
    sell_pct = lend_sell / vol_lots * 100
    if   sell_pct > 3: chip_pts -= 4
    elif sell_pct > 1: chip_pts -= 2

    scores["chips"] = max(0, min(chip_pts, 100))

    # ── 基本面 0~100（基礎分50）────────────────────────────────
    fund_pts = 50
    eg  = yahoo.get("eps_growth") or 0
    roe = yahoo.get("roe") or 0
    dr  = yahoo.get("debt_ratio") or 60  # 沒資料給中性值

    # EPS成長率
    if   eg > 50:  fund_pts += 25
    elif eg > 30:  fund_pts += 18
    elif eg > 20:  fund_pts += 13
    elif eg > 10:  fund_pts += 8
    elif eg >  5:  fund_pts += 4
    elif eg >  0:  fund_pts += 1
    elif eg > -10: fund_pts -= 5
    elif eg > -30: fund_pts -= 12
    else:          fund_pts -= 20

    # ROE
    if   roe > 25: fund_pts += 20
    elif roe > 20: fund_pts += 15
    elif roe > 15: fund_pts += 10
    elif roe > 10: fund_pts += 5
    elif roe >  5: fund_pts += 1
    elif roe <= 0: fund_pts -= 10

    # 負債比
    if   dr < 20: fund_pts += 10
    elif dr < 30: fund_pts += 6
    elif dr < 40: fund_pts += 2
    elif dr > 70: fund_pts -= 8
    elif dr > 80: fund_pts -= 5  # 額外重扣

    scores["fundamental"] = max(0, min(fund_pts, 100))

    # ── 技術面 0~100（基礎分40）────────────────────────────────
    tech_pts = 40
    week52h = yahoo.get("week52_high") or 0
    week52l = yahoo.get("week52_low")  or 0
    if price > 0 and week52h > 0 and week52l > 0:
        # 52週突破
        ratio = price / week52h
        if   ratio >= 1.00: tech_pts += 25
        elif ratio >= 0.95: tech_pts += 15
        elif ratio >= 0.85: tech_pts += 8
        elif ratio >= 0.70: tech_pts += 2
        elif ratio <  0.40: tech_pts -= 15
        elif ratio <  0.55: tech_pts -= 8

        # 52週回撤
        drawdown = (week52h - price) / week52h
        if   drawdown < 0.10: tech_pts += 15
        elif drawdown < 0.20: tech_pts += 8
        elif drawdown < 0.35: tech_pts += 2
        elif drawdown > 0.50: tech_pts -= 12
        if   drawdown > 0.70: tech_pts -= 8

        # 52週波動率
        vol52 = (week52h - week52l) / week52l
        if   vol52 > 1.50: tech_pts += 10
        elif vol52 > 1.00: tech_pts += 6
        elif vol52 > 0.60: tech_pts += 2
        elif vol52 < 0.30: tech_pts -= 5

    # MA60 加分
    if yahoo.get("above_ma60") is True:  tech_pts += 5
    elif yahoo.get("above_ma60") is False: tech_pts -= 5

    scores["technical"] = max(0, min(tech_pts, 100))

    # ── 量能 0~100（基礎分30）──────────────────────────────────
    vol20 = yahoo.get("vol20_avg") or 0
    vol60 = yahoo.get("vol60_avg") or 0
    if vol20 > 0 and vol60 > 0:
        vr = vol20 / vol60
        if   vr > 2.0: vol_pts = 100
        elif vr > 1.5: vol_pts = 85
        elif vr > 1.3: vol_pts = 70
        elif vr > 1.1: vol_pts = 55
        elif vr > 1.0: vol_pts = 40
        elif vr > 0.8: vol_pts = 25
        else:          vol_pts = 10
    else:
        if   vol_month_ratio > 2.0: vol_pts = 90
        elif vol_month_ratio > 1.5: vol_pts = 75
        elif vol_month_ratio > 1.2: vol_pts = 55
        elif vol_month_ratio > 1.0: vol_pts = 40
        elif vol_month_ratio > 0.8: vol_pts = 25
        else:                        vol_pts = 10
    scores["volume"] = vol_pts

    # ── 月營收YoY 0~100（基礎分50）─────────────────────────────
    if revenue_yoy is not None:
        if   revenue_yoy > 40: rev_pts = 100
        elif revenue_yoy > 20: rev_pts = 85
        elif revenue_yoy > 10: rev_pts = 70
        elif revenue_yoy >  5: rev_pts = 60
        elif revenue_yoy >  0: rev_pts = 52
        elif revenue_yoy > -5: rev_pts = 45
        elif revenue_yoy >-20: rev_pts = 30
        else:                   rev_pts = 15
        scores["revenue"] = int(rev_pts)
    else:
        scores["revenue"] = 50  # 無資料給中性分，不影響整體

    # ── RS相對強度 0~100（百分位排名）──────────────────────────
    if rs_pct is not None:
        if   rs_pct >= 90: scores["rs"] = 100
        elif rs_pct >= 80: scores["rs"] = 85
        elif rs_pct >= 70: scores["rs"] = 70
        elif rs_pct >= 50: scores["rs"] = 55
        elif rs_pct >= 30: scores["rs"] = 40
        else:              scores["rs"] = max(10, int(rs_pct * 0.8))
    elif rs_val is not None:
        if   rs_val >  0.20: scores["rs"] = 85
        elif rs_val >  0.10: scores["rs"] = 70
        elif rs_val >  0.03: scores["rs"] = 58
        elif rs_val > -0.03: scores["rs"] = 50
        elif rs_val > -0.10: scores["rs"] = 38
        elif rs_val > -0.20: scores["rs"] = 25
        else:                scores["rs"] = 10
    else:
        scores["rs"] = 50  # 無資料給中性分

    # ── AVWAP 對齊 0~15（三條 AVWAP 各貢獻 5 分）────────────────
    _price_s    = yahoo.get("price") or 0
    _avwap_s    = yahoo.get("avwap_swing")
    _avwap_v    = yahoo.get("avwap_vol")
    _avwap_sh   = yahoo.get("avwap_short")
    _avwap_cnt  = sum([
        bool(_avwap_s  and _price_s >= _avwap_s),
        bool(_avwap_v  and _price_s >= _avwap_v),
        bool(_avwap_sh and _price_s >= _avwap_sh),
    ])
    scores["avwap"] = _avwap_cnt * 5   # 0 / 5 / 10 / 15

    # ── 話題 0~100 ───────────────────────────────────────────
    bull  = sum(1 for n in news_list if n.get("sentiment") == "bullish")
    bear  = sum(1 for n in news_list if n.get("sentiment") == "bearish")
    total = bull + bear
    if total == 0:         topic_pts = 50
    elif bull >= bear * 2: topic_pts = 90
    elif bull > bear:      topic_pts = 70
    elif bull == bear:     topic_pts = 50
    elif bear > bull * 2:  topic_pts = 20
    else:                  topic_pts = 35
    scores["topic"] = topic_pts

    return scores

# ── 7. 三法估值 ───────────────────────────────────────────────────

def calc_fair_price(yahoo):
    eps = yahoo.get("eps_ttm"); eg = yahoo.get("eps_growth") or 0
    if not eps or eps <= 0:
        return {}

    # EPS 成長率壓縮到合理範圍（Yahoo 有時給單季 YoY，極端值不可信）
    eg_capped = max(-20.0, min(eg, 35.0))
    methods = {}

    # ① 歷史PE法（最準，直接反映市場給價歷史）
    if yahoo.get("pe_low") and yahoo.get("pe_mid") and yahoo.get("pe_high"):
        methods["pe"] = {
            "label": "歷史PE法",
            "conservative": round(eps * yahoo["pe_low"],  1),
            "fair":         round(eps * yahoo["pe_mid"],  1),
            "optimistic":   round(eps * yahoo["pe_high"], 1),
        }

    # ② PEG法（正成長才適用，成長率限 3~30）
    if eg_capped > 3:
        peg_pe = max(8.0, min(eg_capped, 30.0))
        methods["peg"] = {
            "label": "PEG法",
            "conservative": round(eps * peg_pe * 0.75, 1),
            "fair":         round(eps * peg_pe,        1),
            "optimistic":   round(eps * peg_pe * 1.25, 1),
        }

    # ③ 市場PE法（台股平均 PE 13~22x，適用所有股票）
    methods["market_pe"] = {
        "label": "市場PE法",
        "conservative": round(eps * 13.0, 1),
        "fair":         round(eps * 17.0, 1),
        "optimistic":   round(eps * 22.0, 1),
    }

    # 取三法中位數（避免單一方法極端值影響結果）
    all_fairs = sorted([v["fair"]         for v in methods.values()])
    all_cons  = sorted([v["conservative"] for v in methods.values()])
    all_opts  = sorted([v["optimistic"]   for v in methods.values()])
    mid = len(all_fairs) // 2
    fp = {
        "conservative": all_cons[mid],
        "fair":         all_fairs[mid],
        "optimistic":   all_opts[mid],
        "methods":      list(methods.values()),
    }
    return fp


# ── 8. Icons ──────────────────────────────────────────────────────

def build_icons(chips, yahoo, vol_month_ratio, news):
    icons = []
    if news: icons.append("📰")
    if (yahoo.get("eps_growth") or 0) > 10 or (yahoo.get("roe") or 0) > 15:
        icons.append("💰")
    if vol_month_ratio > 1.5: icons.append("📊")
    if chips.get("foreign", 0) > 0 or chips.get("trust", 0) > 0:
        icons.append("🏦")
    return icons


# ── 9. 處理單一股票 ───────────────────────────────────────────────

def process_stock(sid, category):
    print(f"    抓取 Yahoo...", end=" ", flush=True)
    yahoo = fetch_yahoo(sid)
    if not yahoo.get("price"):
        print("無股價，跳過")
        return None
    print(f"${yahoo['price']}", end=" ", flush=True)

    # 從快取取中文名稱（TWSE 來源），找不到才用 Yahoo 英文名
    global _benchmark_closes, _revenue_map
    zh_name, zh_industry = fetch_stock_name_industry(sid)
    if zh_name != sid:  # cache 有找到
        yahoo["name"] = zh_name
    # 產業：優先用靜態對照表轉換，cache 沒有才用 Yahoo 英文
    raw_industry = zh_industry if zh_industry != "其他" else yahoo.get("industry", "其他")
    zh = INDUSTRY_MAP.get(raw_industry, "")
    if not zh:
        for en, tw in INDUSTRY_MAP.items():
            if en.lower() in (raw_industry or "").lower():
                zh = tw
                break
    if zh:
        yahoo["industry"] = zh
    elif raw_industry and raw_industry != "其他":
        yahoo["industry"] = raw_industry  # 保留原本（可能已是中文）
    print(f"      [StockInfo] {sid} → {yahoo['name']} / {yahoo['industry']}")

    vol_month_ratio = yahoo.get("vol_month_ratio") or 1.0
    vol_day_ratio   = yahoo.get("vol_day_ratio")   or 1.0

    time.sleep(0.3)
    print("籌碼...", end=" ", flush=True)
    chips = fetch_chips(sid)
    time.sleep(0.4)

    holding = fetch_shareholder(sid)  # 持股功能停用，回傳 None

    print("借券...", end=" ", flush=True)
    lending = fetch_securities_lending(sid)
    time.sleep(0.3)

    if yahoo.get("_is_etf"):
        print(" ETF，跳過"); return None
    print("✓")

    news, themes = [], list(STOCK_THEMES_MAP.get(sid, []))  # 靜態概念表

    # 計算 RS
    rs_val = None
    stock_closes  = yahoo.pop("_closes",  [])
    stock_highs   = yahoo.pop("_highs",   [])
    stock_lows    = yahoo.pop("_lows",    [])
    stock_volumes = yahoo.pop("_volumes", [])
    stock_opens   = yahoo.pop("_opens",   [])
    size_cat  = yahoo.get("size_cat", "mid")
    bm_closes = _benchmark_closes.get(size_cat) or _benchmark_closes.get("mid", [])
    m_z = a_z = rs_trend_stock = rs_slow_positive = None
    if stock_closes and len(bm_closes) >= 240 and len(stock_closes) >= 240:
        try:
            def calc_rs_fn(sp, bp):
                r60  = sp[-1]/sp[-60]  - 1; b60  = bp[-1]/bp[-60]  - 1
                r120 = sp[-1]/sp[-120] - 1; b120 = bp[-1]/bp[-120] - 1
                r240 = sp[-1]/sp[-240] - 1; b240 = bp[-1]/bp[-240] - 1
                return 0.4*(r60-b60) + 0.3*(r120-b120) + 0.3*(r240-b240)
            n = min(len(stock_closes), len(bm_closes))
            rs_val = round(calc_rs_fn(stock_closes[-n:], bm_closes[-n:]), 4)
            yahoo["rs"] = rs_val
        except Exception:
            pass

    # RS 日報酬差序列 + M/A/RS_trend（與類股輪動同頻率、同單位）
    if stock_closes and bm_closes:
        try:
            daily_rs = _compute_rs_layers(stock_closes, bm_closes)
            m_z, a_z, rs_trend_stock = _compute_m_a(daily_rs)
            # 慢速趨勢：近30日日RS均值是否 > 0（對應原來的240日累積方向）
            rs_slow_positive = (sum(daily_rs[-30:]) / 30 > 0) if len(daily_rs) >= 30 else None
        except Exception:
            pass

    revenue_yoy = _revenue_map.get(sid)
    scores     = calc_score(chips, yahoo, vol_month_ratio, news, lending, rs_val, None, revenue_yoy)
    warnings   = calc_warnings(chips, yahoo, vol_month_ratio)
    fair_price = calc_fair_price(yahoo)
    icons      = build_icons(chips, yahoo, vol_month_ratio, news)

    # 基本面顯示列表
    fundamental_rows = [
        {"label": "EPS年增率", "value": f"{yahoo.get('eps_growth','N/A')}%",
         "pass": (yahoo.get("eps_growth") or 0) > 10},
        {"label": "ROE",      "value": f"{yahoo.get('roe','N/A')}%",
         "pass": (yahoo.get("roe") or 0) > 15},
        {"label": "負債比",   "value": f"{yahoo.get('debt_ratio','N/A')}%",
         "pass": (yahoo.get("debt_ratio") or 100) < 50},
    ]

    # 籌碼顯示列表
    chips_rows = []
    if chips:
        for label, net_key, con_key in [("外資","foreign","foreign_con"),("投信","trust","trust_con"),("自營商","dealer","dealer_con")]:
            net = chips.get(net_key, 0); con = chips.get(con_key, 0)
            chips_rows.append({"label": label, "net": net, "consecutive": con,
                               "pass": net > 0 and con > 0})

    return {
        "code":            sid,
        "name":            yahoo.get("name", sid),
        "category":        category,
        "industry":        yahoo.get("industry", "其他"),
        "sector_key":      (STOCK_SECTOR_MAP.get(sid) or
                            SECTOR_KEY_MAP.get(yahoo.get("industry", ""), "") or
                            SECTOR_KEY_MAP.get(INDUSTRY_MAP.get(yahoo.get("industry", ""), ""), "")),
        "sector_rs":       _sector_rotation.get(
                            STOCK_SECTOR_MAP.get(sid) or
                            SECTOR_KEY_MAP.get(yahoo.get("industry", ""), "") or
                            SECTOR_KEY_MAP.get(INDUSTRY_MAP.get(yahoo.get("industry", ""), ""), ""),
                            {}).get("rs", None),
        "themes":          themes,
        "warnings":        warnings,
        "price":           yahoo.get("price"),
        "change":          yahoo.get("change"),
        "change_pct":      yahoo.get("change_pct"),
        "volume":          yahoo.get("volume"),
        "vol_day_ratio":   round(vol_day_ratio, 2),
        "vol_month_ratio": round(vol_month_ratio, 2),
        "week52_high":     yahoo.get("week52_high"),
        "week52_low":      yahoo.get("week52_low"),
        "scores":          scores,
        "icons":           icons,
        "fair_price":      fair_price,
        "eps_ttm":         yahoo.get("eps_ttm"),
        "pe_mid":          yahoo.get("pe_mid"),
        "chips_raw":       {**chips, "shares_outstanding": yahoo.get("shares_outstanding")},
        "holding":         holding,
        "lending":         lending,
        "topic":           {"news": news},
        "fundamental":     fundamental_rows,
        "chips":           chips_rows,
        "yahoo_rs":        rs_val,
        "ma60":            yahoo.get("ma60"),
        "above_ma60":      yahoo.get("above_ma60"),
        "revenue_yoy":     revenue_yoy,
        # 技術面欄位（供訊號計算 + 前端顯示用）
        "open":            yahoo.get("open"),
        "high":            yahoo.get("high"),
        "low":             yahoo.get("low"),
        "prev_close":      yahoo.get("prev_close"),
        "ma5":             yahoo.get("ma5"),
        "ma10":            yahoo.get("ma10"),
        "ma20":            yahoo.get("ma20"),
        "ma_bullish":      yahoo.get("ma_bullish"),
        "high20":          yahoo.get("high20"),
        "low20":           yahoo.get("low20"),
        "avwap_swing":     yahoo.get("avwap_swing"),
        "avwap_vol":       yahoo.get("avwap_vol"),
        "avwap_short":     yahoo.get("avwap_short"),
        "m_z":             round(m_z, 4) if m_z is not None else None,
        "a_z":             round(a_z, 4) if a_z is not None else None,
        "rs_trend_stock":  rs_trend_stock,
        "rs_slow_positive": rs_slow_positive,
        "stock_phase":     "RANGE",  # 在 main() RS 百分位確定後填入
        "signals":         [],       # 在 main() RS 百分位確定後填入
        # 私有欄位供回測用，寫 JSON 前會移除
        "_closes":  stock_closes,
        "_highs":   stock_highs,
        "_lows":    stock_lows,
        "_volumes": stock_volumes,
        "_opens":   stock_opens,
    }


# ── 10. Main ──────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始...")

    # 預先抓三個基準指數（供 RS 計算，period=2y 確保有240天）
    print("  抓取基準指數歷史...")
    global _benchmark_closes
    for key, tid in [("large","0050.TW"), ("mid","^TWII"), ("small","^TWOII")]:
        try:
            bm_hist = yf.Ticker(tid).history(period="2y")
            _benchmark_closes[key] = bm_hist["Close"].tolist()
            print(f"    {key}({tid})：{len(_benchmark_closes[key])} 天")
        except Exception as e:
            print(f"    {key}({tid}) 失敗：{e}")
    time.sleep(1)

    # 先抓外資期貨未平倉（大盤指標）
    print("  抓取外資期貨未平倉...")
    futures_oi = fetch_futures_oi()
    time.sleep(0.5)

    # 動態名單（TWSE）+ 固定大型股
    print("  抓取 TWSE 動態名單...")
    dynamic, twse_stocks = fetch_twse_dynamic()
    time.sleep(1)

    all_ids = list(dict.fromkeys(list(LARGE_CAP) + dynamic))
    # 過濾掉非純數字的代碼（ETF 等）
    # 過濾 ETF（00開頭）、權證（6碼）、憑證、非個股
    ETF_PREFIXES = ("00",)
    all_ids = [s for s in all_ids
               if s.isdigit() and len(s) == 4
               and not any(s.startswith(p) for p in ETF_PREFIXES)
               and int(s) >= 1000]  # 排除指數代碼
    print(f"  總共處理 {len(all_ids)} 檔\n")

    # TWSE STOCK_DAY_ALL 已在 fetch_twse_dynamic() 裡存了名稱到 _name_cache
    # 再從 MI_MARGN 補借券資料（名稱已有，這裡主要是借券）
    fetch_twse_name_lending()
    time.sleep(1)
    print("  抓取借券歷史5日...")
    fetch_lending_history()
    time.sleep(1)
    # 靜態對照表轉換 Yahoo 英文產業為中文
    fetch_twse_industry()
    time.sleep(1)

    # 產業輪動（TWSE 類股指數，60天歷史）
    print("  抓取產業輪動資料...")
    global _sector_rotation
    breadth_map = _compute_sector_breadth(twse_stocks)
    _sector_rotation = fetch_sector_rotation(60, breadth_map=breadth_map)
    time.sleep(1)

    # 大盤相位
    print("  判斷大盤相位...")
    global _market_regime
    _market_regime = fetch_market_regime()
    print(f"  大盤相位：{_market_regime.get('regime')} | 加權 {_market_regime.get('taiex')}")
    time.sleep(0.5)

    # 月營收 YoY（MOPS，不消耗 FinMind 額度）
    print("  抓取月營收...")
    _revenue_map = fetch_mops_revenue()
    print(f"  月營收資料：{len(_revenue_map)} 檔")
    time.sleep(1)

    results = []
    for i, sid in enumerate(all_ids):
        cat = "large" if sid in LARGE_CAP else "mid_small"  # 初始值，process_stock 會用市值覆蓋
        print(f"  [{i+1}/{len(all_ids)}] {sid} [{cat}]")
        r = process_stock(sid, cat)
        if r:
            results.append(r)
        if (i+1) % 10 == 0:
            print("  ── 暫停 15s ──\n"); time.sleep(15)

    # 依綜合評分排序（預設權重）
    def total_score(x):
        s = x.get("scores", {})
        # 話題分數僅供參考不計入
        # 營收/RS 佔位 0 分，之後補資料啟用
        return round(
            s.get("chips",     0) * .35 +
            s.get("fundamental",0) * .30 +
            s.get("revenue",   0) * .05 +
            s.get("volume",    0) * .25 +
            s.get("rs",        0) * .05
        )
    results.sort(key=total_score, reverse=True)

    # 對評分前20名補抓 Gemini 新聞
    print(f"\n  抓取前20名 Gemini 新聞...")
    for i, r in enumerate(results[:20]):
        sid  = r["code"]
        name = r["name"]
        print(f"    [{i+1}/20] {sid} {name}")
        news, themes = fetch_news(name, sid)
        # 靜態概念優先補充
        static_themes = STOCK_THEMES_MAP.get(sid, [])
        for t in static_themes:
            if t not in themes:
                themes.append(t)
        r["topic"]  = {"news": news}
        r["themes"] = themes
        # 補更新話題分數
        r["scores"]["topic"] = calc_score(
            r.get("chips_raw", {}),
            {"eps_growth": None, "roe": None, "debt_ratio": None},
            r.get("vol_month_ratio", 1.0),
            news,
            r.get("lending")
        )["topic"]
        # 補 icons
        r["icons"] = build_icons(
            r.get("chips_raw", {}),
            {"eps_growth": r.get("scores",{}).get("fundamental",0),
             "roe": None},
            r.get("vol_month_ratio", 1.0),
            news
        )
        time.sleep(0.3)

    # RS 百分位排名
    rs_raw = [(r, r.get("yahoo_rs")) for r in results if r.get("yahoo_rs") is not None]
    if rs_raw:
        sorted_rs = sorted(x[1] for x in rs_raw)
        n = len(sorted_rs)
        for r, rv in rs_raw:
            pct = sum(1 for x in sorted_rs if x <= rv) / n * 100
            if   pct >= 90: r["scores"]["rs"] = 100
            elif pct >= 80: r["scores"]["rs"] = 80
            elif pct >= 70: r["scores"]["rs"] = 60
            elif pct >= 50: r["scores"]["rs"] = 40
            else:           r["scores"]["rs"] = max(0, int(pct * 0.6))
            r["rs_pct"] = int(pct)  # 記錄百分位供型態分類用
    # 其餘無 RS 值的股票設預設 rs_pct=50
    for r in results:
        if r.get("rs_pct") is None:
            r["rs_pct"] = 50

    # 型態分類（在 RS 百分位確定後）
    for r in results:
        r["stock_phase"] = classify_stock_phase(
            r.get("rs_pct"),
            r.get("m_z"),
            r.get("a_z"),
            r.get("rs_trend_stock"),
            r.get("rs_slow_positive"),
        )
        # rs_pct_val 供 classify_structure 內部使用
        r["rs_pct_val"] = r.get("rs_pct")

    # 個股結構標籤（多頭/突破準備/回檔/盤整/弱勢）
    for r in results:
        sk         = r.get("sector_key", "")
        sect_phase = _sector_rotation.get(sk, {}).get("sub_phase", "")
        r["structure"] = classify_structure(r, r.get("stock_phase", "RANGE"), sect_phase)

    # 計算買點訊號（型態確定後）
    for r in results:
        if r.get("price"):
            _cs = r.get("scores", {})
            _ws = round(
                _cs.get("chips",       0) * .33 +
                _cs.get("fundamental", 0) * .28 +
                _cs.get("volume",      0) * .23 +
                _cs.get("revenue",     0) * .05 +
                _cs.get("rs",          0) * .05 +
                min(_cs.get("avwap", 0), 15) / 15 * 100 * .06  # avwap 0~15 → 0~100 → 6%
            )
            r["signals"] = calc_signals(
                r, r.get("chips_raw", {}),
                r.get("rs_pct", 50),
                stock_phase=r.get("stock_phase", "RANGE"),
                market_regime=_market_regime.get("regime", "range"),
                composite_score=_ws,
                structure=r.get("structure", ""),
            )

    # ── 提取原始歷史序列供回測，同時從 results 移除（避免寫入 JSON）
    raw_histories = {}
    for r in results:
        cls = r.pop("_closes",  None)
        hgh = r.pop("_highs",   None)
        lws = r.pop("_lows",    None)
        vls = r.pop("_volumes", None)
        opn = r.pop("_opens",   None)
        if cls and hgh and lws and vls:
            raw_histories[r["code"]] = (cls, hgh, lws, vls, opn)

    # ── 歷史勝率回測
    print(f"\n  [回測] 計算歷史訊號勝率（{len(raw_histories)} 支股票）...")
    all_bt = []
    _phase_map = {r["code"]: r.get("stock_phase", "RANGE") for r in results}
    for code, (cls, hgh, lws, vls, opn) in raw_histories.items():
        sp = _phase_map.get(code, "RANGE")
        all_bt.extend(bt_backtest_one_stock(cls, hgh, lws, vls, opens=opn, stock_phase=sp))
    backtest_stats = bt_aggregate_stats(all_bt)
    print(f"  [回測] 樣本：{len(all_bt)} 筆，有效類型：{len(backtest_stats)} 種")

    # ── 信號追蹤更新
    today_str  = datetime.now().strftime("%Y-%m-%d")
    prev_tracking = []
    try:
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                _old = json.load(f)
            prev_tracking = _old.get("signal_tracking", [])
            print(f"  [tracking] 舊追蹤：{len(prev_tracking)} 筆")
    except Exception:
        pass
    today_price_map = {r["code"]: r.get("price") for r in results if r.get("price")}
    today_high_map  = {r["code"]: r.get("high")  for r in results if r.get("high")}
    today_low_map   = {r["code"]: r.get("low")   for r in results if r.get("low")}
    today_open_map  = {r["code"]: r.get("open")  for r in results if r.get("open")}
    signal_tracking = bt_update_tracking(prev_tracking, today_price_map, results, today_str,
                                          sector_rotation=_sector_rotation,
                                          today_high_map=today_high_map,
                                          today_low_map=today_low_map,
                                          today_open_map=today_open_map)
    open_cnt = sum(1 for r in signal_tracking if r.get("status") == "open")
    print(f"  [tracking] 追蹤中：{open_cnt} 筆 | 已結算：{len(signal_tracking) - open_cnt} 筆")

    # 補完 Gemini 後重新排序
    results.sort(key=total_score, reverse=True)

    # ── 讀舊資料，合併歷史借券餘額 + 歷史評分 ──
    old_stocks = {}
    try:
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                old_data = json.load(f)
            for s in old_data.get("stocks", []):
                old_stocks[s["code"]] = s
            print(f"  讀取舊資料：{len(old_stocks)} 檔")
    except Exception as e:
        print(f"  讀取舊資料失敗：{e}")

    today = datetime.now().strftime("%Y-%m-%d")
    for r in results:
        code = r["code"]
        old  = old_stocks.get(code, {})

        # 借券歷史（從 TWSE 直接抓5天，精準）
        hist = _lending_history_cache.get(code, [])
        r["lending_history"] = hist
        if r.get("lending"):
            r["lending"]["daily"] = hist

        # 合併歷史評分（保留近7日）
        old_score_hist = old.get("score_history", [])
        cur_score = r.get("scores", {})
        ws = cur_score.get("chips",0)*.35 + cur_score.get("fundamental",0)*.30 +              cur_score.get("volume",0)*.25 + cur_score.get("revenue",0)*.05 + cur_score.get("rs",0)*.05
        score_entry = {"date": today, "score": round(ws)}
        hist_s = [h for h in old_score_hist if h.get("date") != today]
        hist_s = sorted(hist_s + [score_entry], key=lambda x: x["date"], reverse=True)[:7]
        r["score_history"] = hist_s

    os.makedirs("docs", exist_ok=True)
    _output = {
        "updated_at":      datetime.now().strftime("%Y-%m-%d %H:%M"),
        "market_regime":   _market_regime,
        "sector_rotation": _sector_rotation,
        "total":           len(results),
        "futures_oi":      futures_oi,
        "backtest_stats":  backtest_stats,
        "signal_tracking": signal_tracking,
        "stocks":          results
    }
    # allow_nan=False 確保 NaN/Inf 不寫入（瀏覽器 JSON.parse 不支援）
    import math
    def _clean_nan(obj):
        if isinstance(obj, float):
            return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: _clean_nan(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nan(v) for v in obj]
        return obj
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(_clean_nan(_output), f, ensure_ascii=False, indent=2)
    print(f"\n✅ 完成！共{len(results)}檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
