"""
隨機500股訊號掃描器
資料來源：
  - TWSE STOCK_DAY_ALL：全市場股票 + 流動性篩選（免費無限制）
  - yfinance：OHLC 歷史、均線計算（免費無需 token）
不使用 FinMind / Gemini，完全不消耗付費 API 額度。
"""

import requests, json, time, os, random
import yfinance as yf
from datetime import datetime

OUTPUT_PATH  = "docs/expansion.json"
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; stock-radar-bot/1.0)"}
MIN_VOLUME   = 2_000_000   # 最低成交股數（流動性門檻，約 200 張以上）
MIN_PRICE    = 15          # 最低股價（排除低價股）
SAMPLE_SIZE  = 500         # 每次隨機抽取數量
ETF_PREFIXES = ("00",)


# ── 1. TWSE 全市場資料 ──────────────────────────────────────
def fetch_all_twse_stocks():
    """抓 TWSE STOCK_DAY_ALL，回傳流動性足夠的所有股票"""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        d   = res.json()
        if d.get("stat") != "OK":
            print(f"  [TWSE] 狀態異常: {d.get('stat')}")
            return []

        fields = d.get("fields", [])
        rows   = d.get("data", [])

        try:
            i_code = fields.index("證券代號")
            i_name = fields.index("證券名稱")
            i_vol  = fields.index("成交股數")
            i_cls  = fields.index("收盤價")
            i_open = fields.index("開盤價")
        except ValueError:
            i_code, i_name, i_vol, i_open, i_cls = 0, 1, 2, 5, 8

        result = []
        for r in rows:
            try:
                code = r[i_code].strip()
                if not (code.isdigit() and len(code) == 4):
                    continue
                if any(code.startswith(p) for p in ETF_PREFIXES):
                    continue
                if int(code) < 1000:
                    continue

                vol = int(r[i_vol].replace(",", ""))
                cls = float(r[i_cls].replace(",", "")) if r[i_cls] not in ("--", "") else 0
                opn = float(r[i_open].replace(",", "")) if r[i_open] not in ("--", "") else cls
                chg_pct = round((cls - opn) / opn * 100, 2) if opn > 0 else 0
                name = r[i_name].strip()

                if vol < MIN_VOLUME or cls < MIN_PRICE:
                    continue

                result.append({
                    "code": code,
                    "name": name,
                    "vol":  vol,
                    "price": cls,
                    "chg_pct": chg_pct,
                })
            except:
                continue

        print(f"  [TWSE] 流動性篩選後：{len(result)} 檔")
        return result

    except Exception as e:
        print(f"  [TWSE] 失敗：{e}")
        return []


# ── 2. 讀現有股票代碼（排除用）──────────────────────────────
def load_existing_codes():
    try:
        with open("docs/stocks.json", "r", encoding="utf-8") as f:
            d = json.load(f)
        codes = set(s["code"] for s in d.get("stocks", []))
        print(f"  [existing] 已有 {len(codes)} 檔將被排除")
        return codes
    except Exception as e:
        print(f"  [existing] 讀取失敗：{e}，不排除任何代碼")
        return set()


# ── 3. yfinance 歷史資料 + 均線計算 ─────────────────────────
def fetch_yahoo_data(code):
    """抓 6 個月 OHLCV，計算 MA5/10/20/60 及量比"""
    ticker = yf.Ticker(f"{code}.TW")
    try:
        hist = ticker.history(period="6mo")
        if hist.empty or len(hist) < 20:
            return None

        closes  = hist["Close"].tolist()
        highs   = hist["High"].tolist()
        lows    = hist["Low"].tolist()
        volumes = hist["Volume"].tolist()

        price      = round(closes[-1], 2)
        prev_close = round(closes[-2], 2) if len(closes) >= 2 else price

        def ma(n):
            if len(closes) < n:
                return None
            return round(sum(closes[-n:]) / n, 2)

        high20     = round(max(highs[-20:]),  2)
        low20      = round(min(lows[-20:]),   2)
        prev_low20 = round(min(lows[-21:-1]), 2) if len(lows) >= 21 else low20

        vol_20avg     = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else (volumes[-1] or 1)
        vol_day_ratio = round(volumes[-1] / vol_20avg, 2) if vol_20avg > 0 else 1.0

        return {
            "price":         price,
            "prev_close":    prev_close,
            "ma5":           ma(5),
            "ma10":          ma(10),
            "ma20":          ma(20),
            "ma60":          ma(60),
            "high20":        high20,
            "low20":         low20,
            "prev_low20":    prev_low20,
            "vol_day_ratio": vol_day_ratio,
        }
    except:
        return None


# ── 4. 訊號偵測（純技術面，不依賴籌碼）─────────────────────
def calc_signals(yahoo):
    signals   = []
    price     = yahoo.get("price")     or 0
    high20    = yahoo.get("high20")
    low20     = yahoo.get("low20")
    prev_low20= yahoo.get("prev_low20")
    prev_close= yahoo.get("prev_close")
    ma5       = yahoo.get("ma5")
    ma10      = yahoo.get("ma10")
    ma20      = yahoo.get("ma20")
    ma60      = yahoo.get("ma60")
    vol_day   = yahoo.get("vol_day_ratio") or 1.0

    if not price:
        return []

    def _sig(type_, label, strength, entry, stop, reason):
        risk = round(entry - stop, 2) if stop else 0
        if risk <= 0:
            return None
        return {
            "type":      type_,
            "label":     label,
            "strength":  strength,
            "entry":     round(entry, 2),
            "stop_loss": round(stop, 2),
            "target":    round(entry + risk * 2, 2),
            "risk":      risk,
            "rr":        2.0,
            "reason":    reason,
        }

    # 1. 突破：收盤突破20日高 + 量比≥1.5
    if high20 and price > high20 and vol_day >= 1.5:
        s = _sig("breakout", "突破", "strong", price, low20 or price * 0.95,
                 f"收盤({price})突破20日高({high20})，量比{vol_day:.1f}x")
        if s: signals.append(s)

    # 2. 假跌破：昨收 < prev_low20 且今收 > low20
    if low20 and prev_close and prev_low20 and prev_close < prev_low20 and price > low20:
        s = _sig("false_breakdown", "假跌破", "strong", price, round(low20 * 0.98, 2),
                 f"昨收({prev_close})跌破前20日低，今收({price})強力收復")
        if s: signals.append(s)

    # 3. 均線回測：多頭排列 + 收盤距MA20在3%以內
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20:
        dist = (price - ma20) / ma20
        if 0 <= dist <= 0.03:
            s = _sig("ma_pullback", "均線回測", "medium", price, ma20,
                     f"均線多頭排列，收盤({price})回測MA20({ma20})")
            if s: signals.append(s)

    # 4. 強整再突：緊貼20日高（距離≤5%）+ 收盤 > MA5
    if high20 and ma5 and price > ma5:
        dist = (high20 - price) / high20
        if 0 <= dist <= 0.05:
            s = _sig("high_base", "強整再突", "medium", price,
                     ma10 or round(price * 0.95, 2),
                     f"緊貼20日高({high20})整理，量比{vol_day:.1f}x")
            if s: signals.append(s)

    # 5. 縮量回測：收盤距MA10在2%以內 + 量比<1 + 收盤>MA20
    if ma10 and ma20 and price > ma20:
        dist = abs(price - ma10) / ma10
        if dist <= 0.02 and vol_day < 1.0:
            s = _sig("retest", "縮量回測", "medium", price, ma20,
                     f"縮量({vol_day:.1f}x)回測MA10({ma10})")
            if s: signals.append(s)

    # 6. MA60支撐：收盤距MA60在2%以內
    if ma60:
        dist = (price - ma60) / ma60
        if 0 <= dist <= 0.02:
            s = _sig("ma60_support", "MA60支撐", "weak", price, round(ma60 * 0.97, 2),
                     f"收盤({price})貼近MA60({ma60})")
            if s: signals.append(s)

    return signals


# ── 主程式 ──────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  隨機500股訊號掃描器")
    print("=" * 50)

    # Step 1: 全市場
    print("\n[1] 抓取 TWSE 全市場資料...")
    all_stocks = fetch_all_twse_stocks()
    if not all_stocks:
        print("  無法取得市場資料，終止。")
        return

    # Step 2: 排除現有股票
    print("\n[2] 排除現有股票...")
    existing   = load_existing_codes()
    candidates = [s for s in all_stocks if s["code"] not in existing]
    print(f"  候選池：{len(candidates)} 檔")

    if not candidates:
        print("  候選池為空，終止。")
        return

    # Step 3: 隨機抽樣
    sample_n = min(SAMPLE_SIZE, len(candidates))
    sample   = random.sample(candidates, sample_n)
    print(f"\n[3] 隨機抽取 {sample_n} 檔，開始掃描訊號...\n")

    # Step 4: 逐股掃描
    STRENGTH_ORDER = {"strong": 0, "medium": 1, "weak": 2}
    results = []
    no_data = 0
    no_sig  = 0

    for i, s in enumerate(sample):
        code, name = s["code"], s["name"]
        print(f"  [{i+1:3d}/{sample_n}] {code} {name:<10}", end="  ")

        yahoo = fetch_yahoo_data(code)
        if not yahoo:
            print("skip（無歷史資料）")
            no_data += 1
            time.sleep(0.3)
            continue

        signals = calc_signals(yahoo)
        if not signals:
            print("無訊號")
            no_sig += 1
            time.sleep(0.3)
            continue

        signals.sort(key=lambda x: STRENGTH_ORDER.get(x["strength"], 9))
        labels = [sg["label"] for sg in signals]
        print(f"✓ {len(signals)} 訊號 → {labels}")

        results.append({
            "code":      code,
            "name":      name,
            "price":     yahoo["price"],
            "chg_pct":   s["chg_pct"],
            "vol_ratio": yahoo["vol_day_ratio"],
            "ma5":       yahoo["ma5"],
            "ma20":      yahoo["ma20"],
            "ma60":      yahoo["ma60"],
            "signals":   signals,
        })

        time.sleep(0.4)

    # Step 5: 以最強訊號排序輸出
    results.sort(key=lambda x: (
        min(STRENGTH_ORDER.get(sg["strength"], 9) for sg in x["signals"]),
        -len(x["signals"])
    ))

    output = {
        "updated_at":   datetime.now().strftime("%Y-%m-%d %H:%M"),
        "sample_size":  sample_n,
        "signal_count": len(results),
        "stocks":       results,
    }

    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n{'='*50}")
    print(f"  完成：掃描 {sample_n} 檔")
    print(f"  有訊號：{len(results)} 檔 | 無資料：{no_data} | 無訊號：{no_sig}")
    print(f"  輸出 → {OUTPUT_PATH}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
