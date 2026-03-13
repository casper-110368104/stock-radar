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
    """用 TWSE 公開 API 抓全市場當日資料，取量能/漲跌幅前N名"""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        d = res.json()
        if d.get("stat") != "OK":
            print(f"  [TWSE] 狀態異常: {d.get('stat')}")
            return []

        fields = d.get("fields", [])
        rows   = d.get("data", [])

        # 欄位索引：證券代號/名稱/成交股數/收盤/漲跌
        # 通常欄位：['證券代號','證券名稱','成交股數','成交筆數','成交金額','開盤價','最高價','最低價','收盤價','漲跌(+/-)','漲跌價差','最後揭示買價','最後揭示買量','最後揭示賣價','最後揭示賣量','本益比']
        try:
            i_code = fields.index("證券代號")
            i_vol  = fields.index("成交股數")
            i_cls  = fields.index("收盤價")
            i_open = fields.index("開盤價")
        except ValueError:
            # fallback 固定位置
            i_code, i_vol, i_open, i_cls = 0, 2, 5, 8

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
                stocks.append({"code": code, "vol": vol, "chg": chg})
            except:
                continue

        if not stocks:
            print("  [TWSE] 解析後無資料")
            return []

        by_vol  = sorted(stocks, key=lambda x: x["vol"], reverse=True)
        by_rise = sorted(stocks, key=lambda x: x["chg"], reverse=True)
        by_fall = sorted(stocks, key=lambda x: x["chg"])

        top_vol  = [s["code"] for s in by_vol[:100]]
        top_rise = [s["code"] for s in by_rise[:50]]
        top_fall = [s["code"] for s in by_fall[:30]]

        merged = list(dict.fromkeys(top_vol + top_rise + top_fall))
        print(f"  [TWSE] 量能{len(top_vol)} 漲幅{len(top_rise)} 跌幅{len(top_fall)} → {len(merged)} 檔")
        return merged

    except Exception as e:
        print(f"  [TWSE] 失敗：{e}")
        return []


# ── 2. Yahoo Finance：股價 + 基本面 + 歷史價量 ────────────────────

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
    }
    try:
        ticker = yf.Ticker(f"{sid}.TW")
        info   = ticker.info or {}

        # 股價
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        prev  = info.get("previousClose") or info.get("regularMarketPreviousClose")
        if price and prev:
            result["price"]      = round(float(price), 2)
            result["change"]     = round(float(price) - float(prev), 2)
            result["change_pct"] = round((float(price) - float(prev)) / float(prev) * 100, 2)

        # 名稱和產業
        result["name"]     = info.get("shortName") or info.get("longName") or sid
        result["industry"] = info.get("industry") or info.get("sector") or "其他"

        # 52週
        result["week52_high"] = info.get("fiftyTwoWeekHigh")
        result["week52_low"]  = info.get("fiftyTwoWeekLow")

        # 基本面
        eps = info.get("trailingEps")
        if eps:
            result["eps_ttm"] = round(float(eps), 2)

        roe = info.get("returnOnEquity")
        if roe:
            result["roe"] = round(float(roe) * 100, 1)  # yfinance 給小數

        # 負債比：用 totalDebt / totalAssets
        total_debt   = info.get("totalDebt", 0) or 0
        total_assets = info.get("totalAssets", 0) or 0
        if total_assets > 0:
            result["debt_ratio"] = round(total_debt / total_assets * 100, 1)

        # EPS 年增率：用 earningsGrowth
        eg = info.get("earningsGrowth")
        if eg is not None:
            result["eps_growth"] = round(float(eg) * 100, 1)

        # 歷史價量（月量比 + 歷史PE）
        hist = ticker.history(period="1y")
        if len(hist) >= 20:
            vols = hist["Volume"].tolist()
            today_vol = vols[-1] if vols else 0
            result["volume"] = int(today_vol)

            # 月量比：近20日均量 vs 前3個月均量（第21~80日）
            recent20 = vols[-20:]
            prev60   = vols[-80:-20] if len(vols) >= 80 else vols[:-20]
            avg_recent = sum(recent20) / len(recent20) if recent20 else 0
            avg_prev   = sum(prev60)   / len(prev60)   if prev60   else 0
            result["vol_month_ratio"] = round(avg_recent / avg_prev, 2) if avg_prev > 0 else 1.0

            # 日量比：今日 vs 近20日均量
            result["vol_day_ratio"] = round(today_vol / avg_recent, 2) if avg_recent > 0 else 1.0

            # 歷史PE分位數（用收盤價/EPS估）
            if result["eps_ttm"] and result["eps_ttm"] > 0:
                closes = hist["Close"].tolist()
                pes = [c / result["eps_ttm"] for c in closes if c > 0]
                if len(pes) >= 20:
                    pes_sorted = sorted(pes)
                    n = len(pes_sorted)
                    result["pe_low"]  = round(pes_sorted[int(n*0.15)], 1)
                    result["pe_mid"]  = round(pes_sorted[int(n*0.50)], 1)
                    result["pe_high"] = round(pes_sorted[int(n*0.85)], 1)

    except Exception as e:
        print(f"    [Yahoo] {sid} 失敗：{e}")

    return result


# ── 3. FinMind：三大法人籌碼 ──────────────────────────────────────

def finmind(dataset, stock_id, start_date):
    params = {
        "dataset":    dataset,
        "data_id":    stock_id,
        "start_date": start_date,
        "end_date":   get_date(),
        "token":      FINMIND_TOKEN,
    }
    try:
        res = requests.get(FINMIND_URL, params=params, timeout=25)
        d = res.json()
        if d.get("status") == 200:
            return d.get("data", [])
        print(f"    [FM] {dataset} {stock_id}: {d.get('msg','')}")
    except Exception as e:
        print(f"    [FM] {dataset} {stock_id} 例外：{e}")
    return []

def fetch_chips(sid):
    rows = finmind("TaiwanStockInstitutionalInvestorsBuySell", sid, get_date(40))
    if not rows:
        return {}

    rows.sort(key=lambda x: x.get("date",""))
    latest_date = rows[-1].get("date","")
    latest = [r for r in rows if r.get("date") == latest_date]

    foreign = trust = dealer = 0
    for r in latest:
        name = r.get("name","")
        net  = int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)
        if "外資" in name and "自營" not in name: foreign += net
        elif "投信" in name:                      trust   += net
        elif "自營" in name:                      dealer  += net

    # 連續天數
    def calc_con(name_key):
        days = sorted({r["date"] for r in rows}, reverse=True)
        con = 0
        for d in days:
            day_rows = [r for r in rows if r["date"] == d]
            net = 0
            for r in day_rows:
                n = r.get("name","")
                v = int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)
                if name_key == "foreign" and "外資" in n and "自營" not in n: net += v
                elif name_key == "trust"   and "投信" in n: net += v
                elif name_key == "dealer"  and "自營" in n: net += v
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
        n = r.get("name",""); net = int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)
        if "外資" in n and "自營" not in n: f30 += net
        elif "投信" in n:                  t30 += net
        elif "自營" in n:                  d30 += net

    # 本波段連續累計
    def con_sum(name_key, con_days):
        n = abs(con_days)
        total = 0
        days = sorted({r["date"] for r in rows}, reverse=True)[:n]
        for d in days:
            for r in rows:
                if r["date"] != d: continue
                nm = r.get("name",""); v = int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)
                if name_key == "foreign" and "外資" in nm and "自營" not in nm: total += v
                elif name_key == "trust"  and "投信" in nm: total += v
                elif name_key == "dealer" and "自營" in nm: total += v
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
            n = r.get("name",""); v = int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)
            if "外資" in n and "自營" not in n: df += v
            elif "投信" in n: dt += v
            elif "自營" in n: dd += v
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
    try:
        res = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
            json={"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"temperature":0.3,"maxOutputTokens":600}},
            headers={"Content-Type":"application/json"}, timeout=20
        )
        text = res.json()["candidates"][0]["content"]["parts"][0]["text"]
        text = text.replace("```json","").replace("```","").strip()
        d = json.loads(text)
        return d.get("news", []), d.get("themes", [])
    except Exception as e:
        print(f"    [Gemini] {sid} 失敗：{e}")
        return [], []


# ── 5. 警示訊號 ───────────────────────────────────────────────────

def calc_warnings(chips, yahoo, vol_month_ratio):
    warnings = []
    f_con = chips.get("foreign_con", 0)
    if f_con <= -5:
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


# ── 6. 評分（各項 0~100）─────────────────────────────────────────

def calc_score(chips, yahoo, vol_month_ratio, news_list):
    scores = {}

    # 籌碼 0~100
    chip_pts = 0
    f_net = chips.get("foreign", 0); f_con = chips.get("foreign_con", 0)
    if   f_net > 50000: chip_pts += 50
    elif f_net > 15000: chip_pts += 40
    elif f_net >  5000: chip_pts += 28
    elif f_net >  1000: chip_pts += 15
    elif f_net >     0: chip_pts += 5
    elif f_net <  -500: chip_pts -= 10
    if f_con >= 10:    chip_pts += 20
    elif f_con >= 5:   chip_pts += 12
    elif f_con >= 3:   chip_pts += 5
    elif f_con <= -5:  chip_pts -= 8

    t_net = chips.get("trust", 0); t_con = chips.get("trust_con", 0)
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

    # 基本面 0~100
    fund_pts = 0
    eg  = yahoo.get("eps_growth") or 0
    roe = yahoo.get("roe") or 0
    dr  = yahoo.get("debt_ratio") or 100
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

    # 量能 0~100
    if   vol_month_ratio > 2.0: vol_pts = 100
    elif vol_month_ratio > 1.5: vol_pts = 75
    elif vol_month_ratio > 1.2: vol_pts = 50
    elif vol_month_ratio > 1.0: vol_pts = 30
    elif vol_month_ratio > 0.8: vol_pts = 10
    else:                        vol_pts = 0
    scores["volume"] = vol_pts

    # 話題 0~100
    bull  = sum(1 for n in news_list if n.get("sentiment") == "bullish")
    bear  = sum(1 for n in news_list if n.get("sentiment") == "bearish")
    total = bull + bear
    if total == 0:             topic_pts = 30
    elif bull >= bear * 2:     topic_pts = 100
    elif bull > bear:          topic_pts = 65
    elif bull == bear:         topic_pts = 40
    elif bear > bull * 2:      topic_pts = 0
    else:                      topic_pts = 15
    scores["topic"] = topic_pts

    return scores


# ── 7. 三法估值 ───────────────────────────────────────────────────

def calc_fair_price(yahoo):
    eps = yahoo.get("eps_ttm"); eg = yahoo.get("eps_growth") or 0
    if not eps or eps <= 0:
        return {}
    methods = {}

    # 歷史PE法
    if yahoo.get("pe_low"):
        methods["pe"] = {
            "label": "歷史PE法",
            "conservative": round(eps * yahoo["pe_low"],  1),
            "fair":         round(eps * yahoo["pe_mid"],  1),
            "optimistic":   round(eps * yahoo["pe_high"], 1),
        }

    # PEG法
    if eg > 0:
        peg_pe = max(5.0, min(eg, 40.0))
        methods["peg"] = {
            "label": "PEG法",
            "conservative": round(eps * peg_pe * 0.8, 1),
            "fair":         round(eps * peg_pe,       1),
            "optimistic":   round(eps * peg_pe * 1.3, 1),
        }

    # 葛拉漢法
    g_rate = max(0.0, min(eg, 25.0))
    graham = eps * (8.5 + 2 * g_rate) * 4.4 / 1.875
    if graham > 0:
        methods["graham"] = {
            "label": "葛拉漢法",
            "conservative": round(graham * 0.75, 1),
            "fair":         round(graham,        1),
            "optimistic":   round(graham * 1.35, 1),
        }

    if not methods:
        return {}

    all_con  = [v["conservative"] for v in methods.values()]
    all_opt  = [v["optimistic"]   for v in methods.values()]
    all_fair = [v["fair"]         for v in methods.values()]
    fp = {
        "conservative": max(all_con),
        "fair":         round(sum(all_fair) / len(all_fair), 1),
        "optimistic":   min(all_opt),
        "methods":      list(methods.values()),
    }
    if fp["conservative"] > fp["optimistic"]:
        fp["conservative"], fp["optimistic"] = fp["optimistic"], fp["conservative"]
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

    vol_month_ratio = yahoo.get("vol_month_ratio") or 1.0
    vol_day_ratio   = yahoo.get("vol_day_ratio")   or 1.0

    time.sleep(0.3)
    print("籌碼...", end=" ", flush=True)
    chips = fetch_chips(sid)
    time.sleep(0.4)

    print("新聞...", end=" ", flush=True)
    news, themes = fetch_news(yahoo.get("name", sid), sid)
    time.sleep(0.2)
    print("✓")

    scores     = calc_score(chips, yahoo, vol_month_ratio, news)
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
        "chips_raw":       chips,
        "topic":           {"news": news},
        "fundamental":     fundamental_rows,
        "chips":           chips_rows,
    }


# ── 10. Main ──────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始...")

    # 動態名單（TWSE）+ 固定大型股
    print("  抓取 TWSE 動態名單...")
    dynamic = fetch_twse_dynamic()
    time.sleep(1)

    all_ids = list(dict.fromkeys(list(LARGE_CAP) + dynamic))
    # 過濾掉非純數字的代碼（ETF 等）
    all_ids = [s for s in all_ids if s.isdigit() and len(s) == 4]
    print(f"  總共處理 {len(all_ids)} 檔\n")

    results = []
    for i, sid in enumerate(all_ids):
        cat = "large" if sid in LARGE_CAP else "mid_small"
        print(f"  [{i+1}/{len(all_ids)}] {sid} [{cat}]")
        r = process_stock(sid, cat)
        if r:
            results.append(r)
        if (i+1) % 10 == 0:
            print("  ── 暫停 3s ──\n"); time.sleep(3)

    # 依綜合評分排序（預設權重）
    def total_score(x):
        s = x.get("scores", {})
        return round(s.get("chips",0)*.35 + s.get("fundamental",0)*.30 +
                     s.get("volume",0)*.25 + s.get("topic",0)*.10)
    results.sort(key=total_score, reverse=True)

    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total": len(results),
            "stocks": results
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 完成！共{len(results)}檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
