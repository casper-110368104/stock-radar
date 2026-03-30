"""一次性腳本：將 stocks.json / expansion.json 中殘留的英文 industry 欄位翻成中文"""
import json, pathlib

# 直接複製 INDUSTRY_MAP（避免 import fetch_stocks 需要 yfinance）
INDUSTRY_MAP = {
    "Semiconductors": "半導體", "Semiconductor Equipment & Materials": "半導體設備",
    "Electronic Components": "電子零組件", "Electronics & Computer Hardware": "電腦及週邊",
    "Computer Hardware": "電腦及週邊", "Computer Distribution": "電子通路",
    "Electronics & Computer Distribution": "電子通路",
    "Software - Application": "軟體", "Software - Infrastructure": "軟體",
    "Software": "軟體", "Information Technology Services": "資訊服務",
    "Communication Equipment": "通信網路", "Telecom Services": "電信服務",
    "Telecommunications Services": "電信服務",
    "Consumer Electronics": "消費性電子", "Electronic Gaming & Multimedia": "其他電子",
    "Electrical Equipment & Parts": "電機機械", "Diversified Industrials": "電機機械",
    "Auto Parts": "汽車零組件", "Auto Manufacturers": "汽車",
    "Auto & Truck Dealerships": "汽車",
    "Airlines": "航空", "Marine Shipping": "航運", "Trucking": "航運",
    "Specialty Chemicals": "化學工業", "Chemicals": "化學工業",
    "Steel": "鋼鐵", "Iron & Steel": "鋼鐵",
    "Oil & Gas Equipment & Services": "油電燃氣", "Oil & Gas Integrated": "油電燃氣",
    "Solar": "太陽能", "Utilities - Renewable": "太陽能",
    "Biotechnology": "生技", "Drug Manufacturers": "生技",
    "Medical Devices": "醫療器材", "Health Information Services": "生技",
    "Banks - Regional": "銀行", "Banks - Diversified": "銀行",
    "Financial Conglomerates": "金融控股",
    "Insurance—Life": "壽險", "Insurance - Life": "壽險",
    "Insurance—Property & Casualty": "保險", "Insurance - Property & Casualty": "保險",
    "Insurance Brokers": "保險", "Capital Markets": "證券",
    "Paper & Paper Products": "造紙", "Textile Manufacturing": "紡織",
    "Building Materials": "建材", "Real Estate": "建材營造",
    "Freight & Logistics Services": "航運",
    "Luxury Goods": "貿易百貨", "Department Stores": "貿易百貨",
    "Specialty Retail": "零售百貨", "Grocery Stores": "零售百貨",
    "Restaurants": "餐飲", "Travel & Leisure": "觀光",
    "Tools & Accessories": "其他", "Personal Products": "其他",
    "Internet Content & Information": "其他",
    "Household & Personal Products": "其他",
    "Specialty Business Services": "其他", "Gambling": "觀光",
    "Rental & Leasing Services": "其他", "Shell Companies": "其他",
}

for path in ["docs/stocks.json", "docs/expansion.json"]:
    p = pathlib.Path(path)
    if not p.exists():
        print(f"skip {path}")
        continue
    data = json.loads(p.read_text(encoding="utf-8"))
    changed = 0
    for s in data.get("stocks", []):
        ind = s.get("industry", "")
        # 仍是英文（不含任何中文字）
        if ind and not any('\u4e00' <= c <= '\u9fff' for c in ind):
            zh = INDUSTRY_MAP.get(ind, "")
            if not zh:
                for en, tw in INDUSTRY_MAP.items():
                    if en.lower() in ind.lower():
                        zh = tw
                        break
            if zh:
                s["industry"] = zh
                changed += 1
            else:
                print(f"  WARNING: no mapping for {repr(ind)}")
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{path}: translated {changed} English industry fields")

print("Done.")
