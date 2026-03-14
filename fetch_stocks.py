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
        print(f"  [TWSE] 量能{len(top_vol)} 漲幅{len(top_rise)} 跌幅{len(top_fall)} → {len(merged)} 檔，名稱快取{len(_name_cache)}檔")
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
        result["industry"] = info.get("industry") or info.get("sector") or "其他"

        # 52週
        result["week52_high"] = info.get("fiftyTwoWeekHigh")
        result["week52_low"]  = info.get("fiftyTwoWeekLow")

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
                result["eps_growth"] = round(float(eg) * 100, 1)
                break

        # 歷史價量（月量比 + 歷史PE）
        hist = ticker.history(period="1y")
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
_lending_cache = {}  # {sid: {volume, amount, balance}}

def fetch_twse_industry():
    """從 TWSE isin 網頁抓所有上市公司的中文產業別"""
    global _name_cache
    try:
        from bs4 import BeautifulSoup
        res = requests.get(
            "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=20
        )
        res.encoding = "big5"
        soup = BeautifulSoup(res.text, "lxml")
        rows = soup.select("table.h4 tr")
        ok = 0
        current_ind = ""
        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue
            # 產業別標題列：只有1個td且文字不含數字代碼
            if len(tds) == 1:
                current_ind = tds[0].get_text(strip=True)
                continue
            # 個股列：第一格是「代號＋名稱」（中間有空白）
            if len(tds) >= 4:
                code_name = tds[0].get_text(strip=True)
                parts = code_name.split("　")  # 全形空格
                if not parts:
                    parts = code_name.split()
                code = parts[0].strip() if parts else ""
                if code.isdigit() and len(code) == 4 and current_ind:
                    name = _name_cache.get(code, (code,))[0]
                    _name_cache[code] = (name, current_ind)
                    ok += 1
        print(f"  [TWSE產業] 更新 {ok} 檔中文產業別")
        return ok > 0
    except Exception as e:
        print(f"  [TWSE產業] 失敗：{e}，改用靜態對照表")
        return False


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
                except:
                    continue
            print(f"  [TWSE借券] 名稱{cnt_name}檔 借券{cnt_lend}檔")
            break  # 成功就跳出
        except Exception as e:
            print(f"  [TWSE借券] 失敗：{e}")
            break



# Yahoo Finance industry → 台灣中文產業對照表
INDUSTRY_MAP = {
    # 半導體
    "Semiconductors": "半導體",
    "Semiconductor Equipment & Materials": "半導體",
    "Semiconductor Memory": "半導體",
    # 電子零組件
    "Electronic Components": "電子零組件",
    "Electronic Equipment & Instruments": "電子零組件",
    "Electrical Equipment & Parts": "電子零組件",
    "Electronics Computer & Electronics Equipment": "電子零組件",
    # 電腦及週邊
    "Computer Hardware": "電腦及週邊",
    "Computer Storage & Peripherals": "電腦及週邊",
    "Computers Computer Storage & Peripherals": "電腦及週邊",
    # 通信網路
    "Communication Equipment": "通信網路",
    "Telecom Services": "通信網路",
    "Telecommunications Services": "通信網路",
    # 光電
    "Consumer Electronics": "光電",
    "Electronic Gaming & Multimedia": "光電",
    # 數位雲端
    "Software—Application": "數位雲端",
    "Software—Infrastructure": "數位雲端",
    "Information Technology Services": "資訊服務",
    "Internet Content & Information": "資訊服務",
    # 電機機械
    "Electrical Equipment": "電機機械",
    "Industrial Machinery": "電機機械",
    "Specialty Industrial Machinery": "電機機械",
    "Tools & Accessories": "電機機械",
    "Metal Fabrication": "電機機械",
    # 電器電纜
    "Electrical Components & Equipment": "電器電纜",
    # 汽車
    "Auto Parts": "汽車",
    "Auto Manufacturers": "汽車",
    "Automobiles Components": "汽車",
    # 航運
    "Marine Shipping": "航運",
    "Airlines": "航運",
    "Airports & Air Services": "航運",
    "Trucking": "航運",
    "Integrated Freight & Logistics": "航運",
    # 金融
    "Banks—Regional": "金融",
    "Banks—Diversified": "金融",
    "Financial Conglomerates": "金融",
    "Insurance—Life": "金融",
    "Insurance—Property & Casualty": "金融",
    "Insurance—Diversified": "金融",
    "Capital Markets": "金融",
    "Asset Management": "金融",
    "Credit Services": "金融",
    # 鋼鐵
    "Steel": "鋼鐵",
    "Aluminum": "鋼鐵",
    "Copper": "鋼鐵",
    "Other Industrial Metals & Mining": "鋼鐵",
    # 化學工業
    "Specialty Chemicals": "化學工業",
    "Chemicals": "化學工業",
    "Agricultural Inputs": "化學工業",
    # 生技醫療
    "Biotechnology": "生技醫療",
    "Drug Manufacturers—General": "生技醫療",
    "Drug Manufacturers—Specialty & Generic": "生技醫療",
    "Medical Devices": "生技醫療",
    "Medical Instruments & Supplies": "生技醫療",
    "Healthcare Plans": "生技醫療",
    "Diagnostics & Research": "生技醫療",
    # 塑膠化工
    "Specialty Chemicals": "化學工業",
    "Rubber & Plastics": "塑膠化工",
    "Packaging & Containers": "塑膠化工",
    # 食品
    "Food Distribution": "食品",
    "Packaged Foods": "食品",
    "Beverages—Non-Alcoholic": "食品",
    "Beverages—Brewers": "食品",
    "Agricultural Products & Services": "食品",
    "Farm Products": "食品",
    "Confectioners": "食品",
    "Grocery Stores": "食品",
    # 紡織
    "Apparel Manufacturing": "紡織",
    "Textile Manufacturing": "紡織",
    "Luxury Goods": "紡織",
    "Footwear & Accessories": "紡織",
    # 建材營造
    "Building Materials": "建材營造",
    "Real Estate": "建材營造",
    "Real Estate—Development": "建材營造",
    "Real Estate Services": "建材營造",
    # 觀光餐旅
    "Lodging": "觀光餐旅",
    "Restaurants": "觀光餐旅",
    "Travel Services": "觀光餐旅",
    "Gambling": "觀光餐旅",
    "Entertainment": "觀光餐旅",
    # 貿易百貨
    "Specialty Retail": "貿易百貨",
    "Department Stores": "貿易百貨",
    "Internet Retail": "貿易百貨",
    "Wholesale—Distributors": "貿易百貨",
    # 油電燃氣
    "Oil & Gas Refining & Marketing": "油電燃氣",
    "Oil & Gas Integrated": "油電燃氣",
    "Utilities—Regulated Electric": "油電燃氣",
    "Utilities—Renewable": "綠能環保",
    "Solar": "綠能環保",
    # 橡膠
    "Rubber & Plastics": "橡膠",
    # 水泥
    "Building Products & Equipment": "水泥",
    "Cement & Aggregates": "水泥",
    # 造紙
    "Paper & Paper Products": "造紙",
    "Pulp & Paper": "造紙",
    # 其他
    "Conglomerates": "其他",
    "Business Services": "其他",
    "Staffing & Employment Services": "其他",
    "Security & Protection Services": "其他",
    "Waste Management": "其他",
    "Marine Shipping": "航運",
}

TW_INDUSTRIES = list(set(INDUSTRY_MAP.values()))

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
    rows = finmind("TaiwanStockInstitutionalInvestorsBuySell", sid, get_date(10))
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
            print(f"        {name}: buy={buy} sell={sell} net={(int(buy or 0)-int(sell or 0))//10000}萬")
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
        net  = (buy - sell) // 10000  # 單位：元 → 萬元
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
                v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 10000
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
        n = r.get("name",""); net = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 10000
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
                nm = r.get("name",""); v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 10000
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
            n = r.get("name",""); v = (int(r.get("buy",0) or 0) - int(r.get("sell",0) or 0)) // 10000
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
    models = ["gemini-2.0-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash-lite"]
    for model in models:
        try:
            time.sleep(3)  # 避免超過每分鐘限制
            res = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                json={"contents":[{"parts":[{"text":prompt}]}],
                      "generationConfig":{"temperature":0.3,"maxOutputTokens":600},
                      "safetySettings": safety},
                headers={"Content-Type":"application/json"}, timeout=20
            )
            rj = res.json()
            if "error" in rj:
                print(f"    [Gemini] {sid} {model} 錯誤: {rj['error'].get('message','')[:80]}")
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

# ── 5. 警示訊號 ───────────────────────────────────────────────────

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


# ── 6. 評分（各項 0~100）─────────────────────────────────────────

def calc_score(chips, yahoo, vol_month_ratio, news_list, lending=None):
    scores = {}

    # 籌碼 0~100（單位：萬元）
    chip_pts = 0
    f_net = chips.get("foreign", 0); f_con = chips.get("foreign_con", 0)
    if   f_net >  5000: chip_pts += 50   # >5億
    elif f_net >  1500: chip_pts += 40   # >1.5億
    elif f_net >   500: chip_pts += 28   # >5000萬
    elif f_net >   100: chip_pts += 15   # >1000萬
    elif f_net >     0: chip_pts += 5
    elif f_net <   -50: chip_pts -= 10
    if f_con >= 10:    chip_pts += 20
    elif f_con >= 5:   chip_pts += 12
    elif f_con >= 3:   chip_pts += 5
    elif f_con <= -5:  chip_pts -= 8

    t_net = chips.get("trust", 0); t_con = chips.get("trust_con", 0)
    if   t_net >   500: chip_pts += 20   # >5000萬
    elif t_net >   100: chip_pts += 14   # >1000萬
    elif t_net >    20: chip_pts += 8    # >200萬
    elif t_net >     0: chip_pts += 3
    if t_con >= 5:  chip_pts += 10
    elif t_con >= 3: chip_pts += 5
    elif t_con >= 1: chip_pts += 2

    d_net = chips.get("dealer", 0)
    if d_net > 100: chip_pts += 5
    elif d_net > 0: chip_pts += 2

    # 借券餘額：放空壓力指標（單位：張）
    lend_bal  = lending.get("balance", 0) if lending else 0
    lend_sell = lending.get("volume",  0) if lending else 0
    if   lend_bal >  5000: chip_pts -= 15  # 大量放空
    elif lend_bal >  2000: chip_pts -= 10  # 中量放空
    elif lend_bal >   500: chip_pts -= 5   # 小量放空
    elif lend_bal ==    0: chip_pts += 3   # 完全無借券，正面訊號
    if   lend_sell > 1000: chip_pts -= 5   # 今日大量借券賣出
    elif lend_sell >  300: chip_pts -= 3   # 今日中量借券賣出

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

    # 月營收 0~100（佔位，資料尚未接入）
    scores["revenue"] = 0

    # RS相對強度 0~100（佔位，資料尚未接入）
    scores["rs"] = 0

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
    zh_name, zh_industry = fetch_stock_name_industry(sid)
    if zh_name != sid:  # cache 有找到
        yahoo["name"] = zh_name
    # 產業：Yahoo 的 industry 是英文，但比沒有好；cache 有中文產業就用
    if zh_industry != "其他":
        yahoo["industry"] = zh_industry
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

    print("✓")

    news, themes = [], []  # Gemini 在主流程對前20名補抓
    scores     = calc_score(chips, yahoo, vol_month_ratio, news, lending)
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
        "holding":         holding,
        "lending":         lending,
        "topic":           {"news": news},
        "fundamental":     fundamental_rows,
        "chips":           chips_rows,
    }


# ── 10. Main ──────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始...")

    # 先抓外資期貨未平倉（大盤指標）
    print("  抓取外資期貨未平倉...")
    futures_oi = fetch_futures_oi()
    time.sleep(0.5)

    # 動態名單（TWSE）+ 固定大型股
    print("  抓取 TWSE 動態名單...")
    dynamic = fetch_twse_dynamic()
    time.sleep(1)

    all_ids = list(dict.fromkeys(list(LARGE_CAP) + dynamic))
    # 過濾掉非純數字的代碼（ETF 等）
    all_ids = [s for s in all_ids if s.isdigit() and len(s) == 4]
    print(f"  總共處理 {len(all_ids)} 檔\n")

    # TWSE STOCK_DAY_ALL 已在 fetch_twse_dynamic() 裡存了名稱到 _name_cache
    # 再從 MI_MARGN 補借券資料（名稱已有，這裡主要是借券）
    fetch_twse_name_lending()
    time.sleep(1)
    print("  抓取借券歷史5日...")
    fetch_lending_history()
    time.sleep(1)
    # 先從 TWSE 抓中文產業別（最準確），失敗才用靜態對照表
    fetch_twse_industry()
    time.sleep(1)
    # 靜態對照表補齊還是英文的
    fetch_all_industries(all_ids)
    time.sleep(1)

    results = []
    for i, sid in enumerate(all_ids):
        cat = "large" if sid in LARGE_CAP else "mid_small"
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
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total":      len(results),
            "futures_oi": futures_oi,
            "stocks":     results
        }, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 完成！共{len(results)}檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
