"""
台股選股雷達 v5
- 籌碼：外資/投信/自營 各別張數+連續天數（FinMind單位已是張）
- 新聞：Gemini 判斷利多/利空/中立 + 話題標籤
- 成交量：月量比（近20日均量 vs 前3個月均量）
- 合理股價：EPS × 歷史PE（樂觀/合理/保守）
- 移除：月營收
- 權重評分：籌碼40% 基本面35% 量能15% 話題10%
"""

import requests, json, time, os, xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
OUTPUT_PATH    = "docs/stocks.json"
FINMIND_URL    = "https://api.finmindtrade.com/api/v4/data"
HEADERS        = {"User-Agent": "Mozilla/5.0"}

LARGE_CAP = {
    "2330","2454","2317","2382","2308","2303","2412",
    "2881","2882","2883","2884","2885","2886","2891","2892",
    "1301","1303","1326","2002","2207",
    "2357","2395","2408","2474","3008","3034","3045",
    "3711","4938","5871","6415","6505","6669","8046",
}

# 中小型固定名單已移除，改由每日動態抓取（量能/漲幅/跌幅前N名）
# 如需固定追蹤特定股票，加入 LARGE_CAP 即可


def get_date(days_ago=0):
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def finmind(dataset, stock_id, start_date, end_date=None):
    params = {
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date or get_date(),
        "token": FINMIND_TOKEN,
    }
    if stock_id:
        params["data_id"] = stock_id
    try:
        res = requests.get(FINMIND_URL, params=params, timeout=25)
        d = res.json()
        if d.get("status") == 200:
            return d.get("data", [])
        print(f"    [FM] {dataset} {stock_id}: {d.get('msg')}")
    except Exception as e:
        print(f"    [FM] {dataset} {stock_id} 例外: {e}")
    return []


# ── 1. 股價 ───────────────────────────────────────────

def fetch_price(sid):
    rows = finmind("TaiwanStockPrice", sid, get_date(5))
    if not rows:
        return {}
    r = rows[-1]
    close  = float(r.get("close",  0))
    open_p = float(r.get("open",   0))
    change = round(close - open_p, 2)
    return {
        "price":      close,
        "change":     change,
        "change_pct": round(change / open_p * 100, 2) if open_p else 0,
        "volume":     int(r.get("Trading_Volume", 0)),
        "date":       r.get("date", ""),
    }


# ── 2. 52週高低 ───────────────────────────────────────

def fetch_52w(sid):
    rows = finmind("TaiwanStockPrice", sid, get_date(365))
    closes = [float(r["close"]) for r in rows if r.get("close")]
    if not closes:
        return {}
    return {"week52_high": round(max(closes), 2), "week52_low": round(min(closes), 2)}


# ── 3. 月量比（近20日 vs 前3個月均量）────────────────

def fetch_monthly_vol_ratio(sid, today_vol):
    rows = finmind("TaiwanStockPrice", sid, get_date(100))
    if len(rows) < 25:
        return 0, 0
    vols = [int(r.get("Trading_Volume", 0)) for r in rows]
    recent20  = vols[-20:]
    prev_60   = vols[-80:-20] if len(vols) >= 80 else vols[:-20]
    avg_recent = sum(recent20) / len(recent20) if recent20 else 0
    avg_prev   = sum(prev_60)  / len(prev_60)  if prev_60  else 0
    month_ratio = round(avg_recent / avg_prev, 2) if avg_prev else 0
    day_ratio   = round(today_vol  / avg_recent,  2) if avg_recent else 0
    return month_ratio, day_ratio


# ── 4. 籌碼：外資/投信/自營 ─────────────────────────

def fetch_chips(sid):
    rows = finmind("TaiwanStockInstitutionalInvestorsBuySell", sid, get_date(40))
    empty = {
        "foreign":0,"trust":0,"dealer":0,
        "foreign_con":0,"trust_con":0,"dealer_con":0,
        "foreign_con_sum":0,"trust_con_sum":0,"dealer_con_sum":0,
        "foreign_sum30":0,"trust_sum30":0,"dealer_sum30":0,
        "daily":[],
    }
    if not rows:
        return empty

    daily = defaultdict(lambda: defaultdict(int))
    for r in rows:
        date = r.get("date","")
        name = r.get("name","")
        net  = int(r.get("buy",0)) - int(r.get("sell",0))
        if "外資" in name and "自營" not in name:
            daily[date]["foreign"] += net
        elif "投信" in name:
            daily[date]["trust"]   += net
        elif "自營" in name:
            daily[date]["dealer"]  += net

    dates = sorted(daily.keys(), reverse=True)
    if not dates:
        return empty

    today_data = daily[dates[0]]

    def consec(key):
        count, direction = 0, None
        for d in dates:
            val = daily[d].get(key, 0)
            cur = 1 if val > 0 else (-1 if val < 0 else 0)
            if cur == 0: break
            if direction is None: direction = cur
            if cur == direction: count += 1
            else: break
        return count * (direction or 0)

    def consec_sum(key):
        # 連續買超/賣超期間的累積張數
        total, direction = 0, None
        for d in dates:
            val = daily[d].get(key, 0)
            cur = 1 if val > 0 else (-1 if val < 0 else 0)
            if cur == 0: break
            if direction is None: direction = cur
            if cur == direction: total += val
            else: break
        return total

    # 近30日每日明細
    daily_list = []
    for d in dates[:30]:
        f  = daily[d].get("foreign", 0)
        t  = daily[d].get("trust",   0)
        dl = daily[d].get("dealer",  0)
        daily_list.append({
            "date":    d,
            "foreign": f,
            "trust":   t,
            "dealer":  dl,
            "total":   f + t + dl,
        })

    sum30 = lambda key: sum(daily[d].get(key, 0) for d in dates[:30])

    return {
        "foreign":         today_data.get("foreign", 0),
        "trust":           today_data.get("trust",   0),
        "dealer":          today_data.get("dealer",  0),
        "foreign_con":     consec("foreign"),
        "trust_con":       consec("trust"),
        "dealer_con":      consec("dealer"),
        "foreign_con_sum": consec_sum("foreign"),
        "trust_con_sum":   consec_sum("trust"),
        "dealer_con_sum":  consec_sum("dealer"),
        "foreign_sum30":   sum30("foreign"),
        "trust_sum30":     sum30("trust"),
        "dealer_sum30":    sum30("dealer"),
        "daily":           daily_list,
    }


# ── 5. 基本面：EPS/ROE/負債比 + 歷史PE ───────────────

def fetch_fundamental(sid):
    result = {"eps_ttm": None, "eps_growth": None, "roe": None,
              "debt_ratio": None, "pe_high": None, "pe_low": None, "pe_mid": None}

    # 損益表：ROE + EPS
    fin_rows = finmind("TaiwanStockFinancialStatements", sid, get_date(550))

    roe_data = [r for r in fin_rows if r.get("type") == "ROE"]
    if roe_data:
        result["roe"] = round(float(roe_data[-1].get("value", 0) or 0), 2)

    eps_data = [r for r in fin_rows if r.get("type") == "EPS"]
    if len(eps_data) >= 4:
        # TTM EPS = 最近四季加總
        ttm = sum(float(r.get("value", 0) or 0) for r in eps_data[-4:])
        result["eps_ttm"] = round(ttm, 2)
    if len(eps_data) >= 5:
        eps_now  = float(eps_data[-1].get("value", 0) or 0)
        eps_prev = float(eps_data[-5].get("value", 0) or 0)
        if eps_prev != 0:
            result["eps_growth"] = round((eps_now - eps_prev) / abs(eps_prev) * 100, 1)

    # 資產負債表：負債比
    bs_rows = finmind("TaiwanStockBalanceSheet", sid, get_date(400))
    if bs_rows:
        ld = bs_rows[-1].get("date","")
        latest = [r for r in bs_rows if r.get("date") == ld]
        ta = tl = 0
        for r in latest:
            t = r.get("type",""); v = float(r.get("value",0) or 0)
            if t in ("TotalAssets","資產總額","資產總計"): ta = v
            if t in ("TotalLiabilities","負債總額","負債總計"): tl = v
        if ta > 0 and tl > 0:
            result["debt_ratio"] = round(tl / ta * 100, 2)

    # 歷史PE（近5年）
    pe_rows = finmind("TaiwanStockPER", sid, get_date(365*5))
    pes = [float(r.get("PER", 0)) for r in pe_rows if r.get("PER") and float(r.get("PER",0)) > 0]
    if len(pes) >= 20:
        pes_sorted = sorted(pes)
        n = len(pes_sorted)
        result["pe_low"] = round(pes_sorted[int(n*0.15)], 1)   # 保守（15th percentile）
        result["pe_mid"] = round(pes_sorted[int(n*0.50)], 1)   # 合理（中位數）
        result["pe_high"]= round(pes_sorted[int(n*0.85)], 1)   # 樂觀（85th percentile）

    return result


# ── 6. 新聞 + Gemini（標籤+情緒）────────────────────

def fetch_news(stock_name, sid):
    query = f"{stock_name} {sid} 股票"
    url   = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    items = []
    try:
        res  = requests.get(url, headers=HEADERS, timeout=10)
        root = ET.fromstring(res.content)
        for item in root.findall(".//item")[:5]:
            src = item.find("source")
            items.append({
                "title":     item.findtext("title",""),
                "url":       item.findtext("link",""),
                "source":    src.text if src is not None else "",
                "tag":       "新聞",
                "sentiment": "neutral",
            })
    except Exception as e:
        print(f"    [新聞] 失敗：{e}")
    return items


def analyze_news_gemini(stock_name, news_list):
    if not news_list or not GEMINI_API_KEY:
        return news_list, []
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
        headlines = "\n".join([f"{i+1}. {n['title']}" for i, n in enumerate(news_list)])
        prompt = f"""你是台股分析師。以下是「{stock_name}」的新聞標題，請：

1. 為每則新聞給一個2-4字話題標籤（如：AI、法說、營收、併購、法規、財報）
2. 為每則新聞判斷股價影響：bullish（利多）/ bearish（利空）/ neutral（中立）
3. 綜合所有新聞，判斷這支股票屬於哪些「市場概念/題材」，從以下選擇（可多選，最多5個）：
   AI伺服器、矽光子、低軌衛星、散熱、CoWoS先進封裝、HBM、車用電子、
   電動車、儲能、太陽能、5G、網通、生技新藥、醫材、金融科技、
   航運、鋼鐵原料、半導體設備、ASIC設計、記憶體、面板、
   消費電子、工業電腦、機器人、軍工、其他
   如果不屬於以上任何一個，回傳空陣列。

只回傳JSON物件，格式：
{{"news":[{{"tag":"標籤","sentiment":"bullish|bearish|neutral"}},...], "themes":["概念1","概念2"]}}
不要有其他文字或markdown。

新聞：
{headlines}"""
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        res  = requests.post(url, json=body, timeout=20)
        data = res.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        text = text.strip().replace("```json","").replace("```","").strip()
        parsed = json.loads(text)
        news_parsed = parsed.get("news", [])
        themes      = parsed.get("themes", [])
        for i, n in enumerate(news_list):
            if i < len(news_parsed):
                n["tag"]       = news_parsed[i].get("tag", "新聞")
                n["sentiment"] = news_parsed[i].get("sentiment", "neutral")
        return news_list, themes
    except Exception as e:
        print(f"    [Gemini] 失敗：{e}")
    return news_list, []


# ── 7. 評分（各項 0~100，前端乘權重得最終分）────────

def calc_score(chips, fundamental, vol_month_ratio, news_list):
    scores = {}

    # ── 籌碼 0~100 ──
    # 外資今日（滿50）+ 連續加成（滿20）+ 投信（滿20）+ 自營（滿10）
    chip_pts = 0

    f_net = chips.get("foreign", 0)
    f_con = chips.get("foreign_con", 0)
    if   f_net > 50000: chip_pts += 50
    elif f_net > 15000: chip_pts += 40
    elif f_net >  5000: chip_pts += 28
    elif f_net >  1000: chip_pts += 15
    elif f_net >     0: chip_pts += 5
    elif f_net <  -500: chip_pts -= 10
    if f_con >= 10: chip_pts += 20
    elif f_con >= 5:  chip_pts += 12
    elif f_con >= 3:  chip_pts += 5
    elif f_con <= -5: chip_pts -= 8

    t_net = chips.get("trust", 0)
    t_con = chips.get("trust_con", 0)
    if   t_net >  5000: chip_pts += 20
    elif t_net >  1000: chip_pts += 14
    elif t_net >   200: chip_pts += 8
    elif t_net >     0: chip_pts += 3
    if t_con >= 5:  chip_pts += 10
    elif t_con >= 3: chip_pts += 5
    elif t_con >= 1: chip_pts += 2

    d_net = chips.get("dealer", 0)
    if d_net > 1000: chip_pts += 5
    elif d_net > 0:  chip_pts += 2

    scores["chips"] = max(0, min(chip_pts, 100))

    # ── 基本面 0~100 ──
    # EPS年增率（滿40）+ ROE（滿35）+ 負債比（滿25）
    fund_pts = 0
    eg  = fundamental.get("eps_growth") or 0
    roe = fundamental.get("roe") or 0
    dr  = fundamental.get("debt_ratio") or 100
    if eg > 30:    fund_pts += 40
    elif eg > 15:  fund_pts += 30
    elif eg > 5:   fund_pts += 18
    elif eg > 0:   fund_pts += 8
    elif eg < -10: fund_pts -= 10
    if roe > 20:   fund_pts += 35
    elif roe > 15: fund_pts += 25
    elif roe > 10: fund_pts += 12
    elif roe > 5:  fund_pts += 5
    if dr < 30:    fund_pts += 25
    elif dr < 40:  fund_pts += 18
    elif dr < 60:  fund_pts += 8
    scores["fundamental"] = max(0, min(fund_pts, 100))

    # ── 量能 0~100 ──
    if vol_month_ratio > 2.0:   vol_pts = 100
    elif vol_month_ratio > 1.5: vol_pts = 75
    elif vol_month_ratio > 1.2: vol_pts = 50
    elif vol_month_ratio > 1.0: vol_pts = 30
    elif vol_month_ratio > 0.8: vol_pts = 10
    else:                        vol_pts = 0
    scores["volume"] = vol_pts

    # ── 話題 0~100 ──
    bull = sum(1 for n in news_list if n.get("sentiment") == "bullish")
    bear = sum(1 for n in news_list if n.get("sentiment") == "bearish")
    total = bull + bear
    if total == 0:
        topic_pts = 30  # 無新聞給中性基底
    elif bull >= bear * 2:  topic_pts = 100
    elif bull > bear:       topic_pts = 65
    elif bull == bear:      topic_pts = 40
    elif bear > bull * 2:   topic_pts = 0
    else:                   topic_pts = 15
    scores["topic"] = topic_pts

    # 原始加總（前端會用權重重算）
    scores["total"] = sum(v for k,v in scores.items() if k != "total")
    return scores


# ── 8. 警示訊號 ──────────────────────────────────────

def calc_warnings(chips, fundamental, price_data, vol_month_ratio):
    warnings = []

    # W1：外資連賣超5日以上
    f_con = chips.get("foreign_con", 0)
    if f_con <= -5:
        warnings.append({
            "type": "chips_sell",
            "level": "high",
            "msg": f"外資連賣超{abs(f_con)}日",
        })

    # W2：股價近52週高點（>90%）且量能萎縮（月量比<0.8）
    high = price_data.get("week52_high", 0)
    low  = price_data.get("week52_low", 0)
    price = price_data.get("price", 0)
    if high > low and high > 0:
        pos = (price - low) / (high - low)
        if pos > 0.90 and vol_month_ratio < 0.8:
            warnings.append({
                "type": "high_price_low_vol",
                "level": "mid",
                "msg": f"近52週高點({int(pos*100)}%)且量能萎縮",
            })

    # W3：EPS連續衰退（eps_growth < 0 且上季也衰退）
    eg = fundamental.get("eps_growth", 0) or 0
    if eg < -10:
        warnings.append({
            "type": "eps_decline",
            "level": "mid",
            "msg": f"EPS年增率{eg:.1f}%（衰退）",
        })

    return warnings


# ── 9. Icons ──────────────────────────────────────────

def build_icons(chips, fundamental, vol_month_ratio, news_list):
    icons = []
    bull = sum(1 for n in news_list if n.get("sentiment") == "bullish")
    bear = sum(1 for n in news_list if n.get("sentiment") == "bearish")
    if news_list:
        icons.append("📰")
    if (fundamental.get("eps_growth") or 0) > 10 or (fundamental.get("roe") or 0) > 15:
        icons.append("💰")
    if vol_month_ratio > 1.2:
        icons.append("📊")
    if chips.get("foreign", 0) > 0 or chips.get("trust", 0) > 0:
        icons.append("🏦")
    return icons


# ── 9. 股票資訊 + 熱門清單 ────────────────────────────

def fetch_stock_info():
    """回傳 {stock_id: {name, industry}} 的字典"""
    try:
        rows = finmind("TaiwanStockInfo", "", "2020-01-01")  # 不限日期，確保拿到資料
        result = {}
        for r in rows:
            sid = r.get("stock_id","")
            if sid.isdigit() and len(sid) == 4:
                result[sid] = {
                    "name":     r.get("stock_name", sid),
                    "industry": r.get("industry_category", "其他"),
                }
        return result
    except Exception as e:
        print(f"  [股票資訊] 失敗：{e}"); return {}


def fetch_dynamic_ids():
    """每日動態：用台股總覽抓全市場當日價量，取量能/漲幅/跌幅前N名"""
    try:
        # TaiwanStockPrice 不帶 stock_id 會回傳全市場，但資料量大
        # 改用 TaiwanStockPrice 帶日期範圍抓近3天，再過濾最新日
        rows = finmind("TaiwanStockPrice", "", get_date(5))
        if not rows:
            print("  [動態清單] 無資料，改用空清單")
            return []
        latest = max(r.get("date","") for r in rows)
        today  = [r for r in rows
                  if r.get("date") == latest
                  and r.get("stock_id","").isdigit()
                  and len(r.get("stock_id","")) == 4]
        if not today:
            print(f"  [動態清單] {latest} 無資料")
            return []

        def chg(r):
            try:
                o = float(r.get("open", 0) or 0)
                cl = float(r.get("close", 0) or 0)
                return (cl - o) / o * 100 if o > 0 else 0
            except: return 0

        by_vol  = sorted(today, key=lambda x: float(x.get("Trading_Volume", 0) or 0), reverse=True)
        by_rise = sorted(today, key=chg, reverse=True)
        by_fall = sorted(today, key=chg)

        top_vol  = [r["stock_id"] for r in by_vol[:100]]
        top_rise = [r["stock_id"] for r in by_rise[:50]]
        top_fall = [r["stock_id"] for r in by_fall[:30]]

        merged = list(dict.fromkeys(top_vol + top_rise + top_fall))
        print(f"  動態名單 ({latest})：量能{len(top_vol)} 漲幅{len(top_rise)} 跌幅{len(top_fall)} → {len(merged)} 檔")
        return merged
    except Exception as e:
        print(f"  [動態清單] 失敗：{e}"); return []


# ── 10. 處理單一股票 ──────────────────────────────────

def process_stock(sid, name, category, industry="其他"):
    price = fetch_price(sid)
    if not price: return None
    time.sleep(0.3)

    w52 = fetch_52w(sid); time.sleep(0.2)
    vol_month_ratio, vol_day_ratio = fetch_monthly_vol_ratio(sid, price.get("volume", 0)); time.sleep(0.3)
    chips = fetch_chips(sid); time.sleep(0.3)
    fund  = fetch_fundamental(sid); time.sleep(0.3)
    news  = fetch_news(name, sid)
    news, themes = analyze_news_gemini(name, news)

    scores   = calc_score(chips, fund, vol_month_ratio, news)
    icons    = build_icons(chips, fund, vol_month_ratio, news)
    warnings = calc_warnings(chips, fund, price, vol_month_ratio)

    # 三法估值
    fair_price = {}
    eps = fund.get("eps_ttm")
    eg  = fund.get("eps_growth") or 0   # EPS年增率 %

    methods = {}

    # ① 歷史PE法
    if eps and eps > 0 and fund.get("pe_low"):
        methods["pe"] = {
            "label": "歷史PE法",
            "conservative": round(eps * fund["pe_low"],  1),
            "fair":         round(eps * fund["pe_mid"],  1),
            "optimistic":   round(eps * fund["pe_high"], 1),
        }

    # ② PEG法（Lynch：合理PE = 成長率，成長率限 5~40）
    if eps and eps > 0 and eg > 0:
        peg_pe = max(5.0, min(eg, 40.0))
        methods["peg"] = {
            "label": "PEG法",
            "conservative": round(eps * peg_pe * 0.8, 1),
            "fair":         round(eps * peg_pe,       1),
            "optimistic":   round(eps * peg_pe * 1.3, 1),
        }

    # ③ 葛拉漢公式：V = EPS × (8.5 + 2g) × 4.4 / Y
    #    Y = 台灣10年期公債殖利率，近似 1.875%
    if eps and eps > 0:
        g_rate = max(0.0, min(eg, 25.0))   # 成長率上限 25%
        Y = 1.875
        graham_fair = eps * (8.5 + 2 * g_rate) * 4.4 / Y
        if graham_fair > 0:
            methods["graham"] = {
                "label": "葛拉漢法",
                "conservative": round(graham_fair * 0.75, 1),
                "fair":         round(graham_fair,        1),
                "optimistic":   round(graham_fair * 1.35, 1),
            }

    # 交集區間：max(所有保守值) ~ min(所有樂觀值)
    if methods:
        all_con = [v["conservative"] for v in methods.values()]
        all_opt = [v["optimistic"]   for v in methods.values()]
        all_fair= [v["fair"]         for v in methods.values()]
        fair_price = {
            "conservative": max(all_con),          # 三法中最保守的下限
            "fair":         round(sum(all_fair)/len(all_fair), 1),  # 三法合理均值
            "optimistic":   min(all_opt),          # 三法中最嚴格的上限（交集）
            "methods":      list(methods.values()), # 各法明細
        }
        # 確保 conservative <= fair <= optimistic
        if fair_price["conservative"] > fair_price["optimistic"]:
            fair_price["conservative"], fair_price["optimistic"] = \
                fair_price["optimistic"], fair_price["conservative"]

    # 籌碼格式化
    def chip_row(label, net, con):
        sign = "+" if net >= 0 else ""
        con_str = f"連{'買' if con>0 else '賣'}超{abs(con)}日 " if con != 0 else ""
        return {
            "label": label,
            "net": net,
            "consecutive": con,
            "value": f"{con_str}{sign}{net:,}張",
            "pass": net > 0,
        }

    chips_rows = [
        chip_row("外資",  chips.get("foreign",0), chips.get("foreign_con",0)),
        chip_row("投信",  chips.get("trust",  0), chips.get("trust_con",  0)),
        chip_row("自營商",chips.get("dealer", 0), chips.get("dealer_con", 0)),
    ]

    fundamental_rows = [
        {"label":"EPS年增率","value": f"{fund.get('eps_growth','N/A')}%" if fund.get('eps_growth') is not None else "N/A","pass":(fund.get("eps_growth") or 0)>10},
        {"label":"ROE",      "value": f"{fund.get('roe','N/A')}%"        if fund.get('roe')        is not None else "N/A","pass":(fund.get("roe")        or 0)>15},
        {"label":"負債比",   "value": f"{fund.get('debt_ratio','N/A')}%" if fund.get('debt_ratio') is not None else "N/A","pass":0<(fund.get("debt_ratio") or 100)<50},
    ]

    print(f"    ✓ {name} ${price.get('price')} 總分:{scores['total']} {''.join(icons)}")

    return {
        "code":           sid,
        "name":           name,
        "category":       category,
        "industry":       industry,
        "price":          price.get("price",0),
        "change":         price.get("change",0),
        "change_pct":     price.get("change_pct",0),
        "volume":         price.get("volume",0),
        "vol_day_ratio":  vol_day_ratio,
        "vol_month_ratio":vol_month_ratio,
        "week52_high":    w52.get("week52_high"),
        "week52_low":     w52.get("week52_low"),
        "scores":         scores,
        "icons":          icons,
        "fair_price":     fair_price,
        "eps_ttm":        fund.get("eps_ttm"),
        "pe_mid":         fund.get("pe_mid"),
        "themes":         themes,
        "topic":          {"news": news},
        "fundamental":    fundamental_rows,
        "warnings":       warnings,
        "chips":          chips_rows,
        "chips_raw":      chips,
    }


# ── 11. 主程式 ────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始...")
    if not FINMIND_TOKEN:
        print("錯誤：FINMIND_TOKEN 未設定！"); return

    name_map = fetch_stock_info(); time.sleep(0.5)
    dynamic  = fetch_dynamic_ids(); time.sleep(0.5)

    # LARGE_CAP 固定保留 + 每日動態名單，去重
    all_ids = list(dict.fromkeys(list(LARGE_CAP) + dynamic))
    if name_map:
        all_ids = [s for s in all_ids if s in name_map]
    else:
        print("  ⚠️  name_map 為空，跳過過濾（用代碼當名稱）")
    print(f"  總共處理 {len(all_ids)} 檔\n")

    results = []
    for i, sid in enumerate(all_ids):
        info     = name_map.get(sid, {})
        name     = info.get("name", sid)
        industry = info.get("industry", "其他")
        cat      = "large" if sid in LARGE_CAP else "mid_small"
        print(f"  [{i+1}/{len(all_ids)}] {sid} {name} [{industry}]")
        r = process_stock(sid, name, cat, industry)
        if r: results.append(r)
        if (i+1) % 10 == 0:
            print("  ── 暫停 2s ──"); time.sleep(2)

    # 依總分排序
    results.sort(key=lambda x: x.get("scores",{}).get("total",0), reverse=True)

    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                   "total": len(results), "stocks": results},
                  f, ensure_ascii=False, indent=2)
    print(f"\n✅ 完成！共{len(results)}檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
