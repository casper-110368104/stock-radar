"""
台股選股雷達 - 數據抓取腳本
執行時機：每天收盤後 14:35 以後
輸出：docs/stocks.json（供 GitHub Pages 前端讀取）
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from google import generativeai as genai

# ── 設定區 ──────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "你的API金鑰")

# 你想追蹤的股票清單（股票代碼）
WATCH_LIST = ["2330", "2454", "3661", "2317", "2382", "2308", "3711", "6669"]

OUTPUT_PATH = "docs/stocks.json"
# ────────────────────────────────────────────────────────


def get_today():
    """取得今日日期，格式 YYYYMMDD"""
    return datetime.now().strftime("%Y%m%d")


def get_last_month():
    """取得上個月，格式 YYYYMM"""
    first_day = datetime.now().replace(day=1)
    last_month = first_day - timedelta(days=1)
    return last_month.strftime("%Y%m")


# ── 1. 股價與籌碼（證交所） ────────────────────────────

def fetch_stock_price(stock_id: str) -> dict:
    """
    從證交所抓當日股價與基本資訊
    API文件：https://openapi.twse.com.tw/
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        for item in data:
            if item.get("Code") == stock_id:
                close = float(item.get("ClosingPrice", 0) or 0)
                open_p = float(item.get("OpeningPrice", 0) or 0)
                change = round(close - open_p, 2)
                change_pct = round((change / open_p * 100), 2) if open_p else 0
                return {
                    "price": close,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": int(item.get("TradeVolume", "0").replace(",", "") or 0),
                    "name": item.get("Name", stock_id),
                }
    except Exception as e:
        print(f"[股價] {stock_id} 抓取失敗：{e}")
    return {}


def fetch_chips(stock_id: str) -> dict:
    """
    從證交所抓三大法人買賣超
    API：https://openapi.twse.com.tw/v1/fund/MI_QFIIS
    """
    result = {"foreign_net": 0, "trust_net": 0, "foreign_consecutive": 0, "trust_consecutive": 0}

    # 外資
    try:
        url = "https://openapi.twse.com.tw/v1/fund/MI_QFIIS"
        res = requests.get(url, timeout=10)
        data = res.json()
        for item in data:
            if item.get("StockCode") == stock_id:
                net = item.get("NetBuyShares", "0").replace(",", "")
                result["foreign_net"] = int(net or 0)
                break
    except Exception as e:
        print(f"[外資] {stock_id} 抓取失敗：{e}")

    # 投信
    try:
        url = "https://openapi.twse.com.tw/v1/fund/MI_SITC"
        res = requests.get(url, timeout=10)
        data = res.json()
        for item in data:
            if item.get("StockCode") == stock_id:
                net = item.get("NetBuyShares", "0").replace(",", "")
                result["trust_net"] = int(net or 0)
                break
    except Exception as e:
        print(f"[投信] {stock_id} 抓取失敗：{e}")

    return result


# ── 2. 財報基本面（公開資訊觀測站） ───────────────────

def fetch_fundamental(stock_id: str) -> dict:
    """
    從公開資訊觀測站抓 EPS、ROE、負債比
    使用 MOPS API
    """
    result = {"eps_growth": None, "roe": None, "debt_ratio": None}
    try:
        # 最近一季財務比率
        url = "https://mops.twse.com.tw/mops/web/ajax_t05st22"
        payload = {
            "encodeURIComponent": 1,
            "step": 1,
            "firstin": 1,
            "off": 1,
            "co_id": stock_id,
            "TYPEK": "sii",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded",
                   "Referer": "https://mops.twse.com.tw"}
        res = requests.post(url, data=payload, headers=headers, timeout=15)
        # MOPS 回傳 HTML，需解析（這裡用簡單字串搜尋示意）
        # 實際部署建議用 BeautifulSoup 解析
        html = res.text
        if "股東權益報酬率" in html:
            # 簡易取值示範，實際需 bs4 解析
            result["roe"] = "需bs4解析"
    except Exception as e:
        print(f"[基本面] {stock_id} 抓取失敗：{e}")

    return result


def fetch_revenue(stock_id: str) -> dict:
    """
    從公開資訊觀測站抓月營收
    """
    result = {"mom": None, "yoy": None, "consecutive_growth": 0}
    ym = get_last_month()
    year = int(ym[:4]) - 1911   # 民國年
    month = int(ym[4:])

    try:
        url = "https://mops.twse.com.tw/nas/t21/sii/t21sc03_{year}_{month}_0.html".format(
            year=year, month=month
        )
        res = requests.get(url, timeout=10)
        res.encoding = "big5"
        html = res.text
        # 找到對應股票代碼的行（需 bs4 精確解析）
        if stock_id in html:
            result["yoy"] = "需bs4解析"
    except Exception as e:
        print(f"[營收] {stock_id} 抓取失敗：{e}")

    return result


# ── 3. 新聞話題（Google News RSS + Gemini 分析） ───────

def fetch_news(stock_name: str, stock_id: str) -> list:
    """
    從 Google News RSS 抓最新新聞
    """
    query = f"{stock_name} {stock_id} 股票"
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    news_list = []
    try:
        res = requests.get(url, timeout=10)
        import xml.etree.ElementTree as ET
        root = ET.fromstring(res.content)
        items = root.findall(".//item")[:5]  # 最新5則
        for item in items:
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            source_el = item.find("source")
            source = source_el.text if source_el is not None else "新聞"
            pub_date = item.findtext("pubDate", "")
            news_list.append({
                "title": title,
                "url": link,
                "source": source,
                "date": pub_date[:16] if pub_date else "",
            })
    except Exception as e:
        print(f"[新聞] {stock_name} 抓取失敗：{e}")
    return news_list


def analyze_news_with_gemini(stock_name: str, news_list: list) -> list:
    """
    用 Gemini 分析新聞，判斷話題標籤
    """
    if not news_list or not GEMINI_API_KEY or GEMINI_API_KEY == "你的API金鑰":
        return news_list

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")

    headlines = "\n".join([f"{i+1}. {n['title']}" for i, n in enumerate(news_list)])
    prompt = f"""
以下是 {stock_name} 的最新新聞標題，請為每則新聞給一個簡短的話題標籤（2-4個字，例如：AI、法說、營收、併購、題材、法規、產品）。
只回傳 JSON 陣列，格式：["標籤1","標籤2",...]，不要其他文字。

{headlines}
"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip().replace("```json", "").replace("```", "").strip()
        tags = json.loads(text)
        for i, n in enumerate(news_list):
            n["tag"] = tags[i] if i < len(tags) else "新聞"
    except Exception as e:
        print(f"[Gemini] 分析失敗：{e}")
        for n in news_list:
            n["tag"] = "新聞"

    return news_list


# ── 4. 判斷各項條件是否符合 ───────────────────────────

def evaluate_stock(price_data: dict, chips_data: dict, fundamental: dict, revenue: dict) -> dict:
    """
    根據抓到的數據，判斷每個細項是否符合條件
    """
    return {
        "fundamental": [
            {
                "label": "EPS年增率",
                "value": str(fundamental.get("eps_growth", "N/A")),
                "pass": (fundamental.get("eps_growth") or 0) > 10,
            },
            {
                "label": "ROE",
                "value": str(fundamental.get("roe", "N/A")),
                "pass": (fundamental.get("roe") or 0) > 15,
            },
            {
                "label": "負債比",
                "value": str(fundamental.get("debt_ratio", "N/A")),
                "pass": (fundamental.get("debt_ratio") or 100) < 50,
            },
        ],
        "revenue": [
            {
                "label": "月增率",
                "value": f"{revenue.get('mom', 'N/A')}%",
                "pass": (revenue.get("mom") or -1) > 0,
            },
            {
                "label": "年增率",
                "value": f"{revenue.get('yoy', 'N/A')}%",
                "pass": (revenue.get("yoy") or -1) > 10,
            },
            {
                "label": "連續正成長",
                "value": f"{revenue.get('consecutive_growth', 0)}個月",
                "pass": (revenue.get("consecutive_growth") or 0) >= 3,
            },
        ],
        "chips": [
            {
                "label": "外資",
                "value": "買超" if (chips_data.get("foreign_net") or 0) > 0 else "賣超",
                "pass": (chips_data.get("foreign_net") or 0) > 0,
            },
            {
                "label": "投信",
                "value": "買超" if (chips_data.get("trust_net") or 0) > 0 else "賣超",
                "pass": (chips_data.get("trust_net") or 0) > 0,
            },
        ],
    }


def build_icons(topic_pass: bool, eval_data: dict) -> list:
    icons = []
    if topic_pass:
        icons.append("📰")
    fund_pass = any(i["pass"] for i in eval_data["fundamental"])
    rev_pass = any(i["pass"] for i in eval_data["revenue"])
    chip_pass = any(i["pass"] for i in eval_data["chips"])
    if fund_pass:
        icons.append("💰")
    if rev_pass:
        icons.append("📈")
    if chip_pass:
        icons.append("🏦")
    return icons


# ── 5. 主程式 ─────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 開始抓取台股數據...")
    results = []

    for stock_id in WATCH_LIST:
        print(f"  處理 {stock_id}...")

        # 抓數據
        price_data = fetch_stock_price(stock_id)
        if not price_data:
            print(f"  {stock_id} 股價抓取失敗，跳過")
            continue

        time.sleep(0.5)  # 避免請求太快被擋
        chips_data = fetch_chips(stock_id)
        time.sleep(0.5)
        fundamental = fetch_fundamental(stock_id)
        time.sleep(0.5)
        revenue = fetch_revenue(stock_id)
        time.sleep(0.5)

        stock_name = price_data.get("name", stock_id)
        news_list = fetch_news(stock_name, stock_id)
        news_list = analyze_news_with_gemini(stock_name, news_list)

        # 評估條件
        eval_data = evaluate_stock(price_data, chips_data, fundamental, revenue)
        icons = build_icons(len(news_list) > 0, eval_data)

        results.append({
            "code": stock_id,
            "name": stock_name,
            "price": price_data.get("price", 0),
            "change": price_data.get("change", 0),
            "change_pct": price_data.get("change_pct", 0),
            "icons": icons,
            "topic": {"news": news_list},
            "fundamental": eval_data["fundamental"],
            "revenue": eval_data["revenue"],
            "chips": eval_data["chips"],
        })
        print(f"  {stock_id} {stock_name} 完成，符合圖示：{''.join(icons)}")

    # 輸出 JSON
    os.makedirs("docs", exist_ok=True)
    output = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "stocks": results,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"完成！共處理 {len(results)} 檔股票 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
