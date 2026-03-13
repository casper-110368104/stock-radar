"""
台股選股雷達 - 數據抓取腳本 v2
新增：成交量比、52週高低點、法人連續買賣天數、基本面/營收完整解析
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ── 設定區 ──────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
WATCH_LIST = ["2330", "2454", "3661", "2317", "2382", "2308", "3711", "6669"]
OUTPUT_PATH = "docs/stocks.json"
# ────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9",
}


def get_roc_year():
    """目前民國年"""
    return datetime.now().year - 1911


def get_last_month_roc():
    """上個月，回傳 (民國年, 月)"""
    first = datetime.now().replace(day=1)
    last = first - timedelta(days=1)
    return last.year - 1911, last.month


# ── 1. 全市場股價（一次抓完） ──────────────────────────

def fetch_all_prices() -> dict:
    """
    證交所每日收盤行情（含所有上市股票）
    回傳 {stock_id: {...}} 字典
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        data = res.json()
        result = {}
        for item in data:
            code = item.get("Code", "")
            if not code:
                continue
            try:
                close = float(item.get("ClosingPrice") or 0)
                open_p = float(item.get("OpeningPrice") or 0)
                change = round(close - open_p, 2)
                change_pct = round(change / open_p * 100, 2) if open_p else 0
                vol = int(str(item.get("TradeVolume", "0")).replace(",", "") or 0)
                result[code] = {
                    "name": item.get("Name", code),
                    "price": close,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": vol,
                }
            except (ValueError, TypeError):
                continue
        print(f"  [股價] 共抓到 {len(result)} 檔")
        return result
    except Exception as e:
        print(f"  [股價] 抓取失敗：{e}")
        return {}


# ── 2. 52週高低點 ─────────────────────────────────────

def fetch_52w(stock_id: str) -> dict:
    """
    從 Yahoo Finance 抓52週高低點
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}.TW"
    params = {"range": "1y", "interval": "1d"}
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = res.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            return {
                "week52_high": round(max(closes), 2),
                "week52_low": round(min(closes), 2),
            }
    except Exception as e:
        print(f"  [52週] {stock_id} 失敗：{e}")
    return {}


# ── 3. 均量（計算量比） ───────────────────────────────

def fetch_avg_volume(stock_id: str, days: int = 20) -> float:
    """
    抓近20日均量，用來計算今日量比
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}.TW"
    params = {"range": "3mo", "interval": "1d"}
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = res.json()
        volumes = data["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
        volumes = [v for v in volumes if v is not None]
        if len(volumes) >= days:
            return sum(volumes[-days:]) / days
        elif volumes:
            return sum(volumes) / len(volumes)
    except Exception as e:
        print(f"  [均量] {stock_id} 失敗：{e}")
    return 0


# ── 4. 籌碼面（三大法人 + 連續天數） ─────────────────

def fetch_chips(stock_id: str) -> dict:
    """
    從證交所抓三大法人買賣超
    """
    result = {
        "foreign_net": 0, "trust_net": 0,
        "foreign_consecutive": 0, "trust_consecutive": 0,
    }

    # 外資
    try:
        url = "https://openapi.twse.com.tw/v1/fund/MI_QFIIS"
        res = requests.get(url, headers=HEADERS, timeout=10)
        for item in res.json():
            if item.get("StockCode") == stock_id:
                net = str(item.get("NetBuyShares", "0")).replace(",", "")
                result["foreign_net"] = int(net or 0)
                break
    except Exception as e:
        print(f"  [外資] {stock_id} 失敗：{e}")

    # 投信
    try:
        url = "https://openapi.twse.com.tw/v1/fund/MI_SITC"
        res = requests.get(url, headers=HEADERS, timeout=10)
        for item in res.json():
            if item.get("StockCode") == stock_id:
                net = str(item.get("NetBuyShares", "0")).replace(",", "")
                result["trust_net"] = int(net or 0)
                break
    except Exception as e:
        print(f"  [投信] {stock_id} 失敗：{e}")

    # 連續買賣天數（抓近10日）
    try:
        result["foreign_consecutive"] = fetch_consecutive_days(stock_id, "foreign")
        result["trust_consecutive"] = fetch_consecutive_days(stock_id, "trust")
    except Exception:
        pass

    return result


def fetch_consecutive_days(stock_id: str, chip_type: str) -> int:
    """
    計算外資或投信連續買超/賣超天數
    回傳正數=連續買超N天，負數=連續賣超N天
    """
    today = datetime.now()
    days_checked = 0
    consecutive = 0
    direction = None

    for i in range(1, 11):
        date = today - timedelta(days=i)
        if date.weekday() >= 5:
            continue
        date_str = date.strftime("%Y%m%d")
        try:
            if chip_type == "foreign":
                url = f"https://www.twse.com.tw/fund/QFIIS?response=json&date={date_str}&selectType=ALLBUT0999"
            else:
                url = f"https://www.twse.com.tw/fund/SITC?response=json&date={date_str}&selectType=ALLBUT0999"

            res = requests.get(url, headers=HEADERS, timeout=8)
            data = res.json()
            rows = data.get("data", [])
            for row in rows:
                if row[0] == stock_id:
                    net_str = row[10] if chip_type == "foreign" else row[6]
                    net = int(str(net_str).replace(",", "").replace(" ", "") or 0)
                    cur_dir = 1 if net > 0 else -1
                    if direction is None:
                        direction = cur_dir
                    if cur_dir == direction:
                        consecutive += 1
                    else:
                        return consecutive * direction
                    break
            days_checked += 1
            time.sleep(0.3)
        except Exception:
            continue

    return consecutive * (direction or 0)


# ── 5. 基本面（公開資訊觀測站 MOPS） ─────────────────

def fetch_fundamental(stock_id: str) -> dict:
    """
    從MOPS抓最新一季財務數據
    EPS年增率、ROE、負債比
    """
    result = {"eps_growth": None, "roe": None, "debt_ratio": None}
    year = get_roc_year()

    # 嘗試最近4季
    for q in [4, 3, 2, 1]:
        try:
            url = "https://mops.twse.com.tw/mops/web/ajax_t05st22"
            payload = {
                "encodeURIComponent": "1", "step": "1", "firstin": "1",
                "off": "1", "co_id": stock_id, "TYPEK": "sii",
                "year": str(year - 1911), "season": str(q),
            }
            headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded",
                       "Referer": "https://mops.twse.com.tw"}
            res = requests.post(url, data=payload, headers=headers, timeout=15)
            res.encoding = "utf-8"
            soup = BeautifulSoup(res.text, "lxml")
            tables = soup.find_all("table")

            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if not cells:
                        continue
                    label = cells[0]
                    val = cells[1] if len(cells) > 1 else ""
                    try:
                        num = float(val.replace(",", "").replace("%", "") or 0)
                        if "股東權益報酬率" in label or "ROE" in label:
                            result["roe"] = round(num, 2)
                        elif "負債比率" in label or "負債占資產" in label:
                            result["debt_ratio"] = round(num, 2)
                    except ValueError:
                        pass

            if result["roe"] is not None:
                break
        except Exception as e:
            print(f"  [基本面] {stock_id} Q{q} 失敗：{e}")
            continue

    # EPS年增率：從Yahoo抓兩年EPS比較
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{stock_id}.TW"
        params = {"modules": "incomeStatementHistory"}
        res = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = res.json()
        stmts = data["quoteSummary"]["result"][0]["incomeStatementHistory"]["incomeStatementHistory"]
        if len(stmts) >= 2:
            eps_now = stmts[0].get("dilutedEps", {}).get("raw", 0) or 0
            eps_prev = stmts[1].get("dilutedEps", {}).get("raw", 0) or 0
            if eps_prev and eps_prev != 0:
                result["eps_growth"] = round((eps_now - eps_prev) / abs(eps_prev) * 100, 1)
    except Exception as e:
        print(f"  [EPS] {stock_id} 失敗：{e}")

    return result


# ── 6. 月營收（公開資訊觀測站） ──────────────────────

def fetch_revenue(stock_id: str) -> dict:
    """
    從MOPS抓最近3個月營收，計算月增率、年增率、連續正成長月數
    """
    result = {"mom": None, "yoy": None, "consecutive_growth": 0}
    roc_year, month = get_last_month_roc()

    revenues = []
    for i in range(3):
        m = month - i
        y = roc_year
        if m <= 0:
            m += 12
            y -= 1
        try:
            url = f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{y}_{m:02d}_0.html"
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.encoding = "big5"
            soup = BeautifulSoup(res.text, "lxml")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if cells and cells[0].get_text(strip=True) == stock_id:
                    rev_str = cells[2].get_text(strip=True).replace(",", "")
                    revenues.append(int(rev_str or 0))
                    break
            time.sleep(0.3)
        except Exception as e:
            print(f"  [營收] {stock_id} {y}/{m} 失敗：{e}")

    if len(revenues) >= 2 and revenues[1]:
        result["mom"] = round((revenues[0] - revenues[1]) / revenues[1] * 100, 1)
    if len(revenues) >= 1:
        # 年增率需要去年同期
        try:
            m = month
            y = roc_year - 1
            url = f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{y}_{m:02d}_0.html"
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.encoding = "big5"
            soup = BeautifulSoup(res.text, "lxml")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if cells and cells[0].get_text(strip=True) == stock_id:
                    prev_str = cells[2].get_text(strip=True).replace(",", "")
                    prev = int(prev_str or 0)
                    if prev:
                        result["yoy"] = round((revenues[0] - prev) / prev * 100, 1)
                    break
        except Exception:
            pass

    # 連續正成長月數
    consecutive = 0
    for i in range(len(revenues) - 1):
        if revenues[i] > revenues[i + 1]:
            consecutive += 1
        else:
            break
    result["consecutive_growth"] = consecutive

    return result


# ── 7. 新聞 + Gemini 分析 ─────────────────────────────

def fetch_news(stock_name: str, stock_id: str) -> list:
    query = f"{stock_name} {stock_id}"
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    news_list = []
    try:
        import xml.etree.ElementTree as ET
        res = requests.get(url, headers=HEADERS, timeout=10)
        root = ET.fromstring(res.content)
        for item in root.findall(".//item")[:5]:
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            source_el = item.find("source")
            source = source_el.text if source_el is not None else ""
            news_list.append({"title": title, "url": link, "source": source, "tag": "新聞"})
    except Exception as e:
        print(f"  [新聞] {stock_name} 失敗：{e}")
    return news_list


def analyze_news_gemini(stock_name: str, news_list: list) -> list:
    if not news_list or not GEMINI_API_KEY:
        return news_list
    try:
        from google import generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        headlines = "\n".join([f"{i+1}. {n['title']}" for i, n in enumerate(news_list)])
        prompt = f"""以下是 {stock_name} 的新聞標題，請為每則給一個2-4字的話題標籤（例如：AI、法說、營收、併購、題材、法規）。
只回傳JSON陣列，格式：["標籤1","標籤2",...]，不要其他文字。
{headlines}"""
        response = model.generate_content(prompt)
        text = response.text.strip().replace("```json", "").replace("```", "").strip()
        tags = json.loads(text)
        for i, n in enumerate(news_list):
            n["tag"] = tags[i] if i < len(tags) else "新聞"
    except Exception as e:
        print(f"  [Gemini] 失敗：{e}")
    return news_list


# ── 8. 評估條件 ───────────────────────────────────────

def evaluate(price_data, chips, fundamental, revenue, vol_ratio, w52) -> dict:
    cons_f = chips.get("foreign_consecutive", 0)
    cons_t = chips.get("trust_consecutive", 0)

    return {
        "fundamental": [
            {"label": "EPS年增率", "value": f"{fundamental.get('eps_growth', 'N/A')}%",
             "pass": (fundamental.get("eps_growth") or 0) > 10},
            {"label": "ROE", "value": f"{fundamental.get('roe', 'N/A')}%",
             "pass": (fundamental.get("roe") or 0) > 15},
            {"label": "負債比", "value": f"{fundamental.get('debt_ratio', 'N/A')}%",
             "pass": (fundamental.get("debt_ratio") or 100) < 50},
        ],
        "revenue": [
            {"label": "月增率", "value": f"{revenue.get('mom', 'N/A')}%",
             "pass": (revenue.get("mom") or -1) > 0},
            {"label": "年增率", "value": f"{revenue.get('yoy', 'N/A')}%",
             "pass": (revenue.get("yoy") or -1) > 10},
            {"label": "連續正成長", "value": f"{revenue.get('consecutive_growth', 0)}個月",
             "pass": (revenue.get("consecutive_growth") or 0) >= 3},
        ],
        "chips": [
            {"label": "外資",
             "value": f"連{'買' if cons_f >= 0 else '賣'}超{abs(cons_f)}日" if cons_f else (
                 "買超" if (chips.get("foreign_net") or 0) > 0 else "賣超"),
             "pass": cons_f > 0 or (chips.get("foreign_net") or 0) > 0},
            {"label": "投信",
             "value": f"連{'買' if cons_t >= 0 else '賣'}超{abs(cons_t)}日" if cons_t else (
                 "買超" if (chips.get("trust_net") or 0) > 0 else "賣超"),
             "pass": cons_t > 0 or (chips.get("trust_net") or 0) > 0},
        ],
    }


def build_icons(topic_pass, eval_data) -> list:
    icons = []
    if topic_pass: icons.append("📰")
    if any(i["pass"] for i in eval_data["fundamental"]): icons.append("💰")
    if any(i["pass"] for i in eval_data["revenue"]): icons.append("📈")
    if any(i["pass"] for i in eval_data["chips"]): icons.append("🏦")
    return icons


# ── 9. 主程式 ─────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始抓取台股數據...")

    # 一次抓完所有股價
    all_prices = fetch_all_prices()
    time.sleep(1)

    results = []
    for stock_id in WATCH_LIST:
        print(f"\n  處理 {stock_id}...")
        price_data = all_prices.get(stock_id, {})
        if not price_data:
            print(f"  找不到 {stock_id} 股價，跳過")
            continue

        stock_name = price_data.get("name", stock_id)

        # 52週高低點
        w52 = fetch_52w(stock_id)
        time.sleep(0.5)

        # 均量 → 量比
        avg_vol = fetch_avg_volume(stock_id)
        vol = price_data.get("volume", 0)
        vol_ratio = round(vol / avg_vol, 2) if avg_vol else 0
        time.sleep(0.5)

        # 籌碼
        chips = fetch_chips(stock_id)
        time.sleep(0.5)

        # 基本面
        fundamental = fetch_fundamental(stock_id)
        time.sleep(0.5)

        # 營收
        revenue = fetch_revenue(stock_id)
        time.sleep(0.5)

        # 新聞
        news = fetch_news(stock_name, stock_id)
        news = analyze_news_gemini(stock_name, news)

        # 評估
        eval_data = evaluate(price_data, chips, fundamental, revenue, vol_ratio, w52)
        icons = build_icons(len(news) > 0, eval_data)

        results.append({
            "code": stock_id,
            "name": stock_name,
            "price": price_data.get("price", 0),
            "change": price_data.get("change", 0),
            "change_pct": price_data.get("change_pct", 0),
            "volume": vol,
            "volume_ratio": vol_ratio,
            "week52_high": w52.get("week52_high"),
            "week52_low": w52.get("week52_low"),
            "icons": icons,
            "topic": {"news": news},
            "fundamental": eval_data["fundamental"],
            "revenue": eval_data["revenue"],
            "chips": eval_data["chips"],
        })
        print(f"  完成 {stock_id} {stock_name}｜量比:{vol_ratio}x｜符合:{' '.join(icons)}")

    os.makedirs("docs", exist_ok=True)
    output = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": results}
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完成！共處理 {len(results)} 檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
