#!/usr/bin/env python3
"""
build_sector_map.py — 建立全 TWSE 股票類股對照表

資料來源優先序：
  1. STOCK_SECTOR_MAP（fetch_stocks.py 人工校正，最可靠）
  2. docs/stocks.json（現有股票池的 sector_key）
  3. TWSE opendata API（t187ap03_L，全市場産業別）
  4. TWSE ISIN HTML 解析（備援）

輸出：docs/sector_map.json  →  {code: sector_key}（800+ 檔）
"""
import json, time, requests, re
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0 (stock-radar/1.0)"}
OUT_PATH = "docs/sector_map.json"

# ── TWSE 産業別 → 我們的 sector_key ────────────────────────────────────
TWSE_TO_SECTOR = {
    # 傳統產業
    "水泥工業":         "建材營造",
    "食品工業":         "食品",
    "塑膠工業":         "化學",
    "紡織纖維":         "紡織",
    "電機機械":         "電機機械",
    "電器電纜":         "電機機械",
    "化學工業":         "化學",
    "化學生技醫療":      "生技醫療",
    "玻璃陶瓷":         "建材營造",
    "造紙工業":         "造紙",
    "鋼鐵工業":         "鋼鐵",
    "橡膠工業":         "化學",
    "汽車工業":         "汽車",
    "建材營造業":       "建材營造",
    "航運業":           "航運",
    "觀光事業":         "觀光餐旅",
    "金融保險業":       "金融保險",
    "貿易百貨業":       "貿易百貨",
    "油電燃氣業":       "油電燃氣",
    # 電子
    "電子工業":         "電子工業",
    "半導體業":         "半導體",
    "電腦及週邊設備業":  "電腦週邊",
    "光電業":           "電子零組件",
    "通信網路業":       "通信網路",
    "電子零組件業":     "電子零組件",
    "電子通路業":       "電子通路",
    "資訊服務業":       "資訊服務",
    "其他電子業":       "電子工業",
    # 新興
    "生技醫療業":       "生技醫療",
    "綠能環保":         "綠能環保",
    "數位雲端":         "數位雲端",
    "運動休閒":         "運動休閒",
    "居家生活":         "居家生活",
    "其他":             "其他",
    # 已是最終 key（防止 API 直接回傳 sector_key 字串）
    "建材營造":         "建材營造",
    "航運":             "航運",
    "觀光餐旅":         "觀光餐旅",
    "金融保險":         "金融保險",
    "貿易百貨":         "貿易百貨",
    "油電燃氣":         "油電燃氣",
    "半導體":           "半導體",
    "電腦週邊":         "電腦週邊",
    "通信網路":         "通信網路",
    "電子零組件":       "電子零組件",
    "電子通路":         "電子通路",
    "資訊服務":         "資訊服務",
    "電機機械":         "電機機械",
    "電子工業":         "電子工業",
    "生技醫療":         "生技醫療",
    "造紙":             "造紙",
    "鋼鐵":             "鋼鐵",
    "汽車":             "汽車",
    "食品":             "食品",
    "紡織":             "紡織",
    "化學":             "化學",
}

# ── 人工校正（優先級最高），來自 fetch_stocks.py STOCK_SECTOR_MAP ──────
STOCK_SECTOR_MAP = {
    "1101": "建材營造",  "1213": "食品",      "1235": "食品",
    "1301": "化學",      "1303": "化學",      "1304": "化學",
    "1305": "化學",      "1307": "紡織",      "1308": "化學",
    "1309": "化學",      "1310": "化學",      "1312": "化學",
    "1313": "化學",      "1314": "化學",      "1326": "化學",
    "1402": "紡織",      "1466": "紡織",      "1536": "汽車",
    "1589": "電機機械",  "1597": "電機機械",  "1605": "鋼鐵",
    "1708": "化學",      "1710": "化學",      "1711": "化學",
    "1717": "化學",      "1718": "紡織",      "1723": "化學",
    "1732": "居家生活",  "1736": "運動休閒",  "1760": "生技醫療",
    "1802": "建材營造",  "1904": "造紙",      "1905": "造紙",
    "2002": "鋼鐵",      "2013": "鋼鐵",      "2014": "鋼鐵",
    "2027": "鋼鐵",      "2032": "鋼鐵",      "2038": "鋼鐵",
    "2059": "居家生活",  "2207": "汽車",
    "2303": "半導體",    "2308": "電子零組件", "2313": "電子零組件",
    "2317": "電子零組件","2323": "電腦週邊",  "2324": "電腦週邊",
    "2327": "電子零組件","2329": "半導體",    "2330": "半導體",
    "2337": "半導體",    "2344": "半導體",    "2349": "電腦週邊",
    "2351": "半導體",    "2353": "電腦週邊",  "2357": "電腦週邊",
    "2367": "電子零組件","2369": "半導體",    "2371": "電機機械",
    "2382": "電腦週邊",  "2388": "半導體",    "2395": "電腦週邊",
    "2406": "綠能環保",  "2408": "半導體",    "2409": "電子零組件",
    "2412": "通信網路",  "2424": "通信網路",  "2425": "電腦週邊",
    "2431": "電子零組件","2449": "半導體",    "2454": "半導體",
    "2455": "半導體",    "2474": "電機機械",  "2485": "通信網路",
    "2489": "電機機械",
    "2603": "航運",      "2605": "航運",      "2609": "航運",
    "2610": "航運",      "2615": "航運",      "2616": "航運",
    "2618": "航運",
    "2801": "金融保險",  "2834": "金融保險",  "2867": "金融保險",
    "2880": "金融保險",  "2881": "金融保險",  "2882": "金融保險",
    "2883": "金融保險",  "2884": "金融保險",  "2885": "金融保險",
    "2886": "金融保險",  "2887": "金融保險",  "2890": "金融保險",
    "2891": "金融保險",  "2892": "金融保險",
    "3008": "電子零組件","3026": "電子零組件","3034": "半導體",
    "3041": "半導體",    "3045": "通信網路",  "3049": "電子零組件",
    "3167": "電機機械",  "3231": "電腦週邊",  "3450": "半導體",
    "3481": "電子零組件","3530": "半導體",    "3576": "綠能環保",
    "3583": "半導體",    "3653": "電子零組件","3702": "電子通路",
    "3711": "半導體",    "3715": "電子零組件","3717": "電子零組件",
    "4148": "化學",      "4526": "電機機械",  "4551": "汽車",
    "4566": "電機機械",  "4576": "電機機械",  "4739": "化學",
    "4746": "生技醫療",  "4755": "化學",      "4766": "化學",
    "4906": "通信網路",  "4919": "半導體",    "4927": "電子零組件",
    "4938": "電腦週邊",  "4956": "電子零組件","4958": "電子零組件",
    "4967": "電腦週邊",  "4977": "電腦週邊",  "4989": "鋼鐵",
    "5521": "建材營造",  "5871": "金融保險",  "5880": "金融保險",
    "5906": "紡織",
    "6116": "電子零組件","6155": "電子零組件","6209": "電子零組件",
    "6225": "電子工業",  "6226": "電子零組件","6269": "電子零組件",
    "6282": "電機機械",  "6415": "半導體",    "6438": "電機機械",
    "6443": "綠能環保",  "6451": "半導體",    "6505": "油電燃氣",
    "6550": "生技醫療",  "6585": "化學",      "6669": "電腦週邊",
    "6672": "電子零組件","6689": "數位雲端",  "6722": "汽車",
    "6770": "半導體",    "6789": "半導體",    "6830": "半導體",
    "6890": "紡織",      "6919": "生技醫療",
    "7610": "鋼鐵",      "7711": "電腦週邊",  "7722": "資訊服務",
    "7730": "電機機械",  "7750": "電機機械",  "7780": "食品",
    "8021": "電機機械",  "8046": "電子零組件","8110": "半導體",
    "8112": "半導體",    "8215": "電子零組件","8422": "綠能環保",
    "8940": "觀光餐旅",  "9919": "居家生活",  "9929": "其他",
}


def _to_sector_key(raw: str) -> str:
    """TWSE 産業別字串 → 我們的 sector_key"""
    raw = raw.strip()
    if raw in TWSE_TO_SECTOR:
        return TWSE_TO_SECTOR[raw]
    # 模糊比對：去掉「業」、「工業」等後綴再查
    for key, val in TWSE_TO_SECTOR.items():
        if raw.startswith(key) or key.startswith(raw):
            return val
    return "其他"


def fetch_opendata() -> dict:
    """TWSE opendata t187ap03_L：代號 + 産業類別"""
    url = "https://opendata.twse.com.tw/v1/opendata/t187ap03_L"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        rows = r.json()
        if not isinstance(rows, list):
            print(f"  [opendata] 格式異常：{str(rows)[:100]}")
            return {}
        result = {}
        for row in rows:
            code = str(row.get("公司代號", "")).strip()
            ind  = str(row.get("産業類別", "") or row.get("產業類別", "")).strip()
            if code and code.isdigit() and len(code) == 4 and ind:
                result[code] = _to_sector_key(ind)
        print(f"  [opendata] {len(result)} 檔産業資料")
        return result
    except Exception as e:
        print(f"  [opendata] 失敗：{e}")
        return {}


def fetch_isin_html() -> dict:
    """備援：解析 TWSE ISIN 公開頁面（HTML）"""
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers={**HEADERS, "Referer": "https://isin.twse.com.tw"}, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        result = {}
        for row in soup.select("table.h4 tr"):
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 5:
                continue
            # col[0]: "1234　公司名稱"，col[4]: 産業別
            m = re.match(r"^(\d{4})\s", cols[0])
            if not m:
                continue
            code = m.group(1)
            ind  = cols[4].strip()
            if code and ind:
                result[code] = _to_sector_key(ind)
        print(f"  [ISIN HTML] {len(result)} 檔産業資料")
        return result
    except Exception as e:
        print(f"  [ISIN HTML] 失敗：{e}")
        return {}


def load_stocks_json() -> dict:
    """從 docs/stocks.json 取 code→sector_key"""
    try:
        with open("docs/stocks.json", encoding="utf-8") as f:
            sj = json.load(f)
        result = {}
        for s in sj.get("stocks", []):
            c  = s.get("code", "")
            sk = s.get("sector_key", "")
            if c and sk:
                result[c] = sk
        print(f"  [stocks.json] {len(result)} 檔")
        return result
    except Exception as e:
        print(f"  [stocks.json] 失敗：{e}")
        return {}


def main():
    print("=" * 60)
    print("  build_sector_map — 建立全 TWSE 類股對照表")
    print("=" * 60)

    # 1) 最低優先：TWSE opendata API
    sector_map = fetch_opendata()

    # 2) ISIN HTML 備援（若 opendata 資料不足）
    if len(sector_map) < 500:
        print("  opendata 資料不足，嘗試 ISIN HTML 備援...")
        isin = fetch_isin_html()
        for code, sk in isin.items():
            if code not in sector_map:
                sector_map[code] = sk

    # 3) stocks.json 覆蓋（已手動校正的條目）
    stocks_sk = load_stocks_json()
    overridden = 0
    for code, sk in stocks_sk.items():
        if sector_map.get(code) != sk:
            overridden += 1
        sector_map[code] = sk
    print(f"  [stocks.json] 覆蓋 {overridden} 筆不同值")

    # 4) 人工校正（最高優先，絕對覆蓋）
    manual_updated = 0
    for code, sk in STOCK_SECTOR_MAP.items():
        if sector_map.get(code) != sk:
            manual_updated += 1
        sector_map[code] = sk
    print(f"  [STOCK_SECTOR_MAP] 覆蓋 {manual_updated} 筆不同值")

    # 5) 只保留 4 碼整數代號
    sector_map = {
        code: sk for code, sk in sector_map.items()
        if code.isdigit() and len(code) == 4 and int(code) >= 1000
        and not code.startswith("00") and sk
    }

    print(f"\n  ✓ 最終：{len(sector_map)} 檔有 sector_key")

    # 板塊分布
    from collections import Counter
    dist = Counter(sector_map.values())
    for sk, cnt in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"    {sk:20s}: {cnt}")

    # 儲存
    Path("docs").mkdir(exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(sector_map, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"\n  → 已寫入 {OUT_PATH}")


if __name__ == "__main__":
    main()
