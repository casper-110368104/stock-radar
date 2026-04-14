import json, pathlib

for path in ["docs/stocks.json", "docs/expansion.json"]:
    p = pathlib.Path(path)
    if not p.exists():
        print(f"skip {path}")
        continue
    data = json.loads(p.read_text(encoding="utf-8"))
    before_t = len(data.get("signal_tracking", []))
    before_a = len(data.get("signal_archive",  []))
    data["signal_tracking"] = []
    data["signal_archive"]  = []
    data["backtest_stats"]  = {}
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{path}: tracking {before_t}→0, archive {before_a}→0")

print("Done.")
