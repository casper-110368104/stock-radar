"""
台股選股雷達 - 數據抓取腳本 v3
數據來源：FinMind API（穩定、不封鎖 GitHub Actions）
修正：Gemini 模型更新為 gemini-2.0-flash
"""

import requests
import json
import time
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# ── 設定區 ──────────────────────────────────────────────
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
WATCH_LIST = ["2330", "2454", "3661", "2317", "2382", "2308", "3711", "6669"]
OUTPUT_PATH = "docs/stocks.json"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
# ────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_date(days_ago=0):
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def finmind(dataset, stock_id, start_date, end_date=None):
    """FinMind API 通用呼叫"""
    params = {
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date or get_date(),
        "token": FINMIND_TOKEN,
    }
    try:
        res = requests.get(FINMIND_URL, params=params, timeout=20)
        data = res.json()
        if data.get("status") == 200:
            return data.get("data", [])
        else:
            print(f"  [FinMind] {dataset} {stock_id} 失敗：{data.get('msg')}")
    except Exception as e:
        print(f"  [FinMind] {dataset} {stock_id} 例外：{e}")
    return []


# ── 1. 股價與基本資訊 ─────────────────────────────────

def fetch_price(stock_id):
    rows = finmind("TaiwanStockPrice", stock_id, get_date(5))
    if not rows:
        return {}
    latest = rows[-1]
    close = float(latest.get("close", 0))
    open_p = float(latest.get("open", 0))
    change = round(close - open_p, 2)
    change_pct = round(change / open_p * 100, 2) if open_p else 0
    volume = int(latest.get("Trading_Volume", 0))
    return {
        "price": close,
        "change": change,
        "change_pct": change_pct,
        "volume": volume,
        "date": latest.get("date", ""),
    }


# ── 2. 52週高低點 ─────────────────────────────────────

def fetch_52w(stock_id):
    rows = finmind("TaiwanStockPrice", stock_id, get_date(365))
    if not rows:
        return {}
    closes = [float(r["close"]) for r in rows if r.get("close")]
    if not closes:
        return {}
    return {
        "week52_high": round(max(closes), 2),
        "week52_low": round(min(closes), 2),
    }


# ── 3. 均量與量比 ─────────────────────────────────────

def fetch_volume_ratio(stock_id, today_vol):
    rows = finmind("TaiwanStockPrice", stock_id, get_date(40))
    if len(rows) < 5:
        return 0
    vols = [int(r.get("Trading_Volume", 0)) for r in rows[:-1]][-20:]
    avg = sum(vols) / len(vols) if vols else 0
    return round(today_vol / avg, 2) if avg else 0


# ── 4. 籌碼面（三大法人） ────────────────────────────

def fetch_chips(stock_id):
    rows = finmind("TaiwanStockInstitutionalInvestorsBuySell", stock_id, get_date(30))
    if not rows:
        return {"foreign_net": 0, "trust_net": 0,
                "foreign_consecutive": 0, "trust_consecutive": 0}

    # 整理成日期→各法人買賣超
    from collections import defaultdict
    daily = defaultdict(dict)
    for r in rows:
        date = r.get("date", "")
        name = r.get("name", "")
        net = int(r.get("buy", 0)) - int(r.get("sell", 0))
        if "外資" in name:
            daily[date]["foreign"] = net
        elif "投信" in name:
            daily[date]["trust"] = net

    dates = sorted(daily.keys(), reverse=True)

    # 今日數據
    today = daily.get(dates[0], {}) if dates else {}
    foreign_net = today.get("foreign", 0)
    trust_net = today.get("trust", 0)

    # 連續買賣天數
    def consecutive(key):
        count = 0
        direction = None
        for d in dates:
            val = daily[d].get(key, 0)
            cur = 1 if val > 0 else (-1 if val < 0 else 0)
            if cur == 0:
                break
            if direction is None:
                direction = cur
            if cur == direction:
                count += 1
            else:
                break
        return count * (direction or 0)

    return {
        "foreign_net": foreign_net,
        "trust_net": trust_net,
        "foreign_consecutive": consecutive("foreign"),
        "trust_consecutive": consecutive("trust"),
    }


# ── 5. 基本面（財務比率） ────────────────────────────

def fetch_fundamental(stock_id):
    result = {"eps_growth": None, "roe": None, "debt_ratio": None}

    # ROE、負債比
    rows = finmind("TaiwanStockFinancialRatios", stock_id, get_date(400))
    if rows:
        latest = rows[-1]
        result["roe"] = round(float(latest.get("ReturnOnEquity", 0) or 0), 2)
        result["debt_ratio"] = round(float(latest.get("DebtRatio", 0) or 0), 2)

    # EPS年增率
    eps_rows = finmind("TaiwanStockFinancialStatements", stock_id, get_date(500))
    eps_data = [r for r in eps_rows if r.get("type") == "EPS"]
    if len(eps_data) >= 2:
        eps_now = float(eps_data[-1].get("value", 0) or 0)
        eps_prev = float(eps_data[-5].get("value", 0) or 0) if len(eps_data) >= 5 else float(eps_data[0].get("value", 0) or 0)
        if eps_prev and eps_prev != 0:
            result["eps_growth"] = round((eps_now - eps_prev) / abs(eps_prev) * 100, 1)

    return result


# ── 6. 月營收 ────────────────────────────────────────

def fetch_revenue(stock_id):
    result = {"mom": None, "yoy": None, "consecutive_growth": 0}
    rows = finmind("TaiwanStockMonthRevenue", stock_id, get_date(400))
    if not rows:
        return result

    rows = sorted(rows, key=lambda x: (x.get("date", "")))
    revs = [int(r.get("revenue", 0) or 0) for r in rows]

    if len(revs) >= 2 and revs[-2]:
        result["mom"] = round((revs[-1] - revs[-2]) / revs[-2] * 100, 1)

    if len(revs) >= 13 and revs[-13]:
        result["yoy"] = round((revs[-1] - revs[-13]) / revs[-13] * 100, 1)

    # 連續正成長月數
    count = 0
    for i in range(len(revs) - 1, 0, -1):
        if revs[i] > revs[i - 1]:
            count += 1
        else:
            break
    result["consecutive_growth"] = count

    return result


# ── 7. 新聞 + Gemini 分析 ─────────────────────────────

def fetch_news(stock_name, stock_id):
    query = f"{stock_name} {stock_id} 股票"
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    news_list = []
    try:
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


def analyze_news_gemini(stock_name, news_list):
    if not news_list or not GEMINI_API_KEY:
        return news_list
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
        headlines = "\n".join([f"{i+1}. {n['title']}" for i, n in enumerate(news_list)])
        prompt = f"""以下是 {stock_name} 的新聞標題，請為每則給一個2-4字的話題標籤（例如：AI、法說、營收、併購、題材、法規）。
只回傳JSON陣列，格式：["標籤1","標籤2",...]，不要其他文字。
{headlines}"""
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        res = requests.post(url, json=body, timeout=15)
        data = res.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        text = text.strip().replace("```json", "").replace("```", "").strip()
        tags = json.loads(text)
        for i, n in enumerate(news_list):
            n["tag"] = tags[i] if i < len(tags) else "新聞"
    except Exception as e:
        print(f"  [Gemini] 失敗：{e}")
    return news_list


# ── 8. 評估條件 ───────────────────────────────────────

def evaluate(chips, fundamental, revenue):
    cons_f = chips.get("foreign_consecutive", 0)
    cons_t = chips.get("trust_consecutive", 0)

    def chip_label(consecutive, net):
        if consecutive != 0:
            return f"連{'買' if consecutive > 0 else '賣'}超{abs(consecutive)}日"
        return "買超" if net > 0 else "賣超"

    return {
        "fundamental": [
            {"label": "EPS年增率",
             "value": f"{fundamental.get('eps_growth', 'N/A')}%" if fundamental.get('eps_growth') is not None else "N/A",
             "pass": (fundamental.get("eps_growth") or 0) > 10},
            {"label": "ROE",
             "value": f"{fundamental.get('roe', 'N/A')}%" if fundamental.get('roe') is not None else "N/A",
             "pass": (fundamental.get("roe") or 0) > 15},
            {"label": "負債比",
             "value": f"{fundamental.get('debt_ratio', 'N/A')}%" if fundamental.get('debt_ratio') is not None else "N/A",
             "pass": 0 < (fundamental.get("debt_ratio") or 100) < 50},
        ],
        "revenue": [
            {"label": "月增率",
             "value": f"{revenue.get('mom', 'N/A')}%" if revenue.get('mom') is not None else "N/A",
             "pass": (revenue.get("mom") or -1) > 0},
            {"label": "年增率",
             "value": f"{revenue.get('yoy', 'N/A')}%" if revenue.get('yoy') is not None else "N/A",
             "pass": (revenue.get("yoy") or -1) > 10},
            {"label": "連續正成長",
             "value": f"{revenue.get('consecutive_growth', 0)}個月",
             "pass": (revenue.get("consecutive_growth") or 0) >= 3},
        ],
        "chips": [
            {"label": "外資",
             "value": chip_label(cons_f, chips.get("foreign_net", 0)),
             "pass": cons_f > 0 or (chips.get("foreign_net") or 0) > 0},
            {"label": "投信",
             "value": chip_label(cons_t, chips.get("trust_net", 0)),
             "pass": cons_t > 0 or (chips.get("trust_net") or 0) > 0},
        ],
    }


def build_icons(topic_pass, eval_data):
    icons = []
    if topic_pass: icons.append("📰")
    if any(i["pass"] for i in eval_data["fundamental"]): icons.append("💰")
    if any(i["pass"] for i in eval_data["revenue"]): icons.append("📈")
    if any(i["pass"] for i in eval_data["chips"]): icons.append("🏦")
    return icons


# ── 9. 股票名稱對照表 ─────────────────────────────────

def fetch_stock_names():
    try:
        rows = finmind("TaiwanStockInfo", "", get_date(1))
        return {r["stock_id"]: r["stock_name"] for r in rows if r.get("stock_id")}
    except Exception:
        return {}


# ── 10. 主程式 ────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始抓取台股數據...")

    if not FINMIND_TOKEN:
        print("錯誤：FINMIND_TOKEN 未設定！")
        return

    # 股票名稱對照
    name_map = fetch_stock_names()

    results = []
    for stock_id in WATCH_LIST:
        print(f"\n  處理 {stock_id}...")
        stock_name = name_map.get(stock_id, stock_id)

        # 股價
        price_data = fetch_price(stock_id)
        if not price_data:
            print(f"  {stock_id} 股價抓取失敗，跳過")
            continue
        time.sleep(0.5)

        # 52週
        w52 = fetch_52w(stock_id)
        time.sleep(0.3)

        # 量比
        vol_ratio = fetch_volume_ratio(stock_id, price_data.get("volume", 0))
        time.sleep(0.3)

        # 籌碼
        chips = fetch_chips(stock_id)
        time.sleep(0.3)

        # 基本面
        fundamental = fetch_fundamental(stock_id)
        time.sleep(0.3)

        # 營收
        revenue = fetch_revenue(stock_id)
        time.sleep(0.3)

        # 新聞
        news = fetch_news(stock_name, stock_id)
        news = analyze_news_gemini(stock_name, news)

        # 評估
        eval_data = evaluate(chips, fundamental, revenue)
        icons = build_icons(len(news) > 0, eval_data)

        results.append({
            "code": stock_id,
            "name": stock_name,
            "price": price_data.get("price", 0),
            "change": price_data.get("change", 0),
            "change_pct": price_data.get("change_pct", 0),
            "volume": price_data.get("volume", 0),
            "volume_ratio": vol_ratio,
            "week52_high": w52.get("week52_high"),
            "week52_low": w52.get("week52_low"),
            "icons": icons,
            "topic": {"news": news},
            "fundamental": eval_data["fundamental"],
            "revenue": eval_data["revenue"],
            "chips": eval_data["chips"],
        })
        print(f"  完成 {stock_id} {stock_name}｜量比:{vol_ratio}x｜符合:{''.join(icons)}")

    os.makedirs("docs", exist_ok=True)
    output = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "stocks": results,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完成！共處理 {len(results)} 檔 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
