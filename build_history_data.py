#!/usr/bin/env python3
"""
build_history_data.py — 下載歷史三大法人籌碼 + 月營收

輸出：
  docs/chips_history.json  {"YYYY-MM-DD": {"2330": {"f": 1234, "fc": 3, "t": 56, "tc": 1, "d": 78}, ...}, ...}
  docs/revenue_history.json {"2330": {"2022-01": 15.3, ...}, ...}（如有 FINMIND_TOKEN）

下載範圍：
  三大法人：2022-01-01 到今天
  月營收：2021-12-01 起（FinMind，需 FINMIND_TOKEN）

執行：python build_history_data.py
支援中斷後繼續（incremental）：已下載日期會從 chips_history.json 讀取跳過。
"""
import json, os, time, requests
from datetime import date, timedelta, datetime
from pathlib import Path

# ── 設定 ─────────────────────────────────────────────────────────────
CHIPS_START     = date(2022, 1, 1)
CHIPS_END       = date.today()
REVENUE_START   = "2021-12-01"
CHIPS_OUT       = Path("docs/chips_history.json")
REVENUE_OUT     = Path("docs/revenue_history.json")
TWSE_T86_URL    = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date}&selectType=ALL"
FINMIND_URL     = "https://api.finmindtrade.com/api/v4/data"
HEADERS         = {"User-Agent": "Mozilla/5.0 (stock-radar-build/1.0)"}
SLEEP_BETWEEN   = 0.35   # 每個日期請求後 sleep（秒）


# ── 讀取已有資料（支援 incremental）──────────────────────────────────
def _load_existing_chips():
    if CHIPS_OUT.exists():
        with open(CHIPS_OUT, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _load_existing_revenue():
    if REVENUE_OUT.exists():
        with open(REVENUE_OUT, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ── 計算連買/連賣天數 ──────────────────────────────────────────────
def _update_consecutive(prev_cons, net_val):
    """
    f_net > 0 → 連買累加（+）；< 0 → 連賣累加（負數）；= 0 維持前日
    prev_cons: 前一日的連買天數（正=連買，負=連賣，0=初始）
    """
    if net_val > 0:
        return prev_cons + 1 if prev_cons >= 0 else 1
    elif net_val < 0:
        return prev_cons - 1 if prev_cons <= 0 else -1
    else:
        return prev_cons


# ── TWSE T86 單日請求 ─────────────────────────────────────────────
def _fetch_t86_day(date_str):
    """
    date_str: "YYYYMMDD"
    返回 {code: {"f": float, "t": float, "d": float}} 或 None（假日/資料缺失）
    單位：T86 是股（shares），函式內除以 1000 轉換成張（lots）
    """
    url = TWSE_T86_URL.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        d = r.json()
    except Exception as e:
        print(f"    T86 {date_str} 請求失敗：{e}")
        return None

    if d.get("stat") != "OK":
        return None   # 假日或無資料

    fields = d.get("fields", [])
    rows   = d.get("data", [])
    if not rows:
        return None

    # 動態找欄位 index（避免 TWSE API 欄位順序異動）
    def _fi(name):
        for i, f in enumerate(fields):
            if name in f:
                return i
        return None

    i_code   = _fi("證券代號")
    i_f_net  = _fi("外陸資買賣超股數")   # 外資（含陸資）淨買超
    i_t_net  = _fi("投信買賣超股數")      # 投信淨買超
    i_d_net  = _fi("自營商買賣超股數")    # 自營商淨買超

    if i_code is None or i_f_net is None or i_t_net is None or i_d_net is None:
        # fallback：hardcode index（T86 的傳統欄位順序）
        i_code  = 0
        i_f_net = 4
        i_t_net = 7
        i_d_net = 8

    def _parse(val):
        if val == "--" or val == "---":
            return 0.0
        try:
            return float(str(val).replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

    result = {}
    for row in rows:
        try:
            code = str(row[i_code]).strip()
            if not (code.isdigit() and len(code) == 4):
                continue
            f_shares = _parse(row[i_f_net])
            t_shares = _parse(row[i_t_net])
            d_shares = _parse(row[i_d_net])
            # 股 → 張（1 張 = 1000 股）
            result[code] = {
                "f": round(f_shares / 1000, 2),
                "t": round(t_shares / 1000, 2),
                "d": round(d_shares / 1000, 2),
            }
        except (IndexError, Exception):
            continue

    return result if result else None


# ── 計算連買/連賣（對所有日期按時序計算）────────────────────────────
def _build_consecutive(chips_hist):
    """
    輸入：chips_hist = {date_str: {code: {"f": ..., "t": ..., "d": ...}}}
    輸出：在每個 code 的 dict 裡補上 "fc"（外資連買天數）、"tc"（投信連買天數）
    原地修改 chips_hist。
    """
    # 按日期排序
    sorted_dates = sorted(chips_hist.keys())

    # 追蹤各股連買天數
    f_con = {}   # code → 外資連買天數
    t_con = {}   # code → 投信連買天數

    for ds in sorted_dates:
        day_data = chips_hist[ds]
        new_day  = {}
        for code, v in day_data.items():
            prev_fc = f_con.get(code, 0)
            prev_tc = t_con.get(code, 0)
            new_fc  = _update_consecutive(prev_fc, v.get("f", 0))
            new_tc  = _update_consecutive(prev_tc, v.get("t", 0))
            f_con[code] = new_fc
            t_con[code] = new_tc
            new_day[code] = {
                "f":  v.get("f", 0),
                "fc": new_fc,
                "t":  v.get("t", 0),
                "tc": new_tc,
                "d":  v.get("d", 0),
            }
        chips_hist[ds] = new_day

    return chips_hist


# ── FinMind 月營收 ────────────────────────────────────────────────
def _fetch_revenue_finmind(token):
    """
    從 FinMind 下載 TaiwanStockMonthRevenue，計算 YoY。
    返回 {code: {"YYYY-MM": yoy_pct}}
    """
    print(f"  [月營收] 從 FinMind 下載...")
    params = {
        "dataset":   "TaiwanStockMonthRevenue",
        "start_date": REVENUE_START,
        "token":      token,
    }
    try:
        r = requests.get(FINMIND_URL, params=params, headers=HEADERS, timeout=60)
        d = r.json()
    except Exception as e:
        print(f"  [月營收] FinMind 請求失敗：{e}")
        return {}

    if d.get("status") != 200:
        print(f"  [月營收] FinMind 非 200：{d.get('status')} {d.get('msg','')}")
        return {}

    data = d.get("data", [])
    if not data:
        print(f"  [月營收] FinMind 無資料")
        return {}

    print(f"  [月營收] 取得 {len(data)} 筆原始資料")

    # 整理：{code: {YYYY-MM: revenue}}
    raw = {}
    for row in data:
        code     = str(row.get("stock_id", "")).strip()
        date_str = str(row.get("date", ""))[:7]   # "YYYY-MM"
        rev      = row.get("revenue", 0) or 0
        if not code or not date_str:
            continue
        raw.setdefault(code, {})[date_str] = float(rev)

    # 計算 YoY
    result = {}
    for code, months in raw.items():
        yoy_dict = {}
        for ym, rev in months.items():
            try:
                yr, mo = int(ym[:4]), int(ym[5:7])
                prev_ym = f"{yr-1}-{mo:02d}"
                prev_rev = months.get(prev_ym)
                if prev_rev and prev_rev > 0 and rev > 0:
                    yoy = round((rev / prev_rev - 1) * 100, 2)
                    yoy_dict[ym] = yoy
            except Exception:
                continue
        if yoy_dict:
            result[code] = yoy_dict

    print(f"  [月營收] 計算完成：{len(result)} 檔有 YoY 資料")
    return result


# ── 主程式 ───────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  build_history_data.py — 歷史籌碼 + 月營收")
    print(f"  籌碼範圍：{CHIPS_START} ~ {CHIPS_END}")
    print("=" * 60)

    # 確保 docs/ 存在
    CHIPS_OUT.parent.mkdir(parents=True, exist_ok=True)

    # ── 載入已有資料（incremental）
    chips_hist = _load_existing_chips()
    existing_dates = set(chips_hist.keys())
    print(f"  [籌碼] 已有 {len(existing_dates)} 個交易日資料，跳過")

    # ── 產生所有要下載的日期
    all_dates = []
    cur = CHIPS_START
    while cur <= CHIPS_END:
        ds = cur.strftime("%Y-%m-%d")
        if ds not in existing_dates:
            all_dates.append(cur)
        cur += timedelta(days=1)

    print(f"  [籌碼] 需下載 {len(all_dates)} 個日期")

    # ── 逐日下載
    # 先把已有資料中的連買天數暫時移除（重算時會重建）
    # 為了重算連買天數，先去掉已有資料中的 fc/tc（保留 f/t/d）
    for ds, day_data in chips_hist.items():
        for code, v in day_data.items():
            chips_hist[ds][code] = {
                "f": v.get("f", 0),
                "t": v.get("t", 0),
                "d": v.get("d", 0),
            }

    new_count  = 0
    skip_count = 0
    for i, cur_date in enumerate(all_dates):
        date_str = cur_date.strftime("%Y%m%d")   # TWSE format: YYYYMMDD
        ds       = cur_date.strftime("%Y-%m-%d")  # JSON key format

        day_data = _fetch_t86_day(date_str)
        if day_data is None:
            skip_count += 1
        else:
            # 只存 f/t/d（fc/tc 後面統一重算）
            chips_hist[ds] = {code: {"f": v["f"], "t": v["t"], "d": v["d"]}
                               for code, v in day_data.items()}
            new_count += 1
            print(f"  [{i+1:4d}/{len(all_dates)}] {ds}  {len(day_data)} 檔")

        time.sleep(SLEEP_BETWEEN)

        # 每 200 個日期儲存一次（防止中斷丟失）
        if (i + 1) % 200 == 0:
            _build_consecutive(chips_hist)
            with open(CHIPS_OUT, "w", encoding="utf-8") as f:
                json.dump(chips_hist, f, ensure_ascii=False)
            print(f"  [自動存檔] {CHIPS_OUT}（{len(chips_hist)} 個交易日）")
            # 重設連買天數中間狀態：下一批重算前先把 fc/tc 去掉（會在下次 _build 時重算）
            for ds2, day2 in chips_hist.items():
                for code2, v2 in day2.items():
                    chips_hist[ds2][code2] = {
                        "f": v2.get("f", 0),
                        "t": v2.get("t", 0),
                        "d": v2.get("d", 0),
                    }

    print(f"\n  [籌碼] 下載完成：新增 {new_count} 日，跳過 {skip_count} 日（假日等）")

    # ── 計算連買/連賣天數（全量重算）
    print(f"  [籌碼] 計算連買/連賣天數...")
    chips_hist = _build_consecutive(chips_hist)

    # ── 寫出 chips_history.json
    with open(CHIPS_OUT, "w", encoding="utf-8") as f:
        json.dump(chips_hist, f, ensure_ascii=False)
    print(f"  [籌碼] 已寫入 {CHIPS_OUT}（{len(chips_hist)} 個交易日）")

    # ── 月營收（FinMind，需 token）
    finmind_token = os.environ.get("FINMIND_TOKEN", "")
    if finmind_token:
        rev_hist = _fetch_revenue_finmind(finmind_token)
        if rev_hist:
            with open(REVENUE_OUT, "w", encoding="utf-8") as f:
                json.dump(rev_hist, f, ensure_ascii=False)
            print(f"  [月營收] 已寫入 {REVENUE_OUT}（{len(rev_hist)} 檔）")
        else:
            print(f"  [月營收] 無資料，跳過寫入")
    else:
        print(f"  [月營收] 未設定 FINMIND_TOKEN，跳過")

    print("\n  完成！")


if __name__ == "__main__":
    main()
