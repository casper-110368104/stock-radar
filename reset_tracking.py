import json, pathlib

for path in ["docs/stocks.json", "docs/expansion.json"]:
    p = pathlib.Path(path)
    if not p.exists():
        print(f"skip {path}")
        continue
    data = json.loads(p.read_text(encoding="utf-8"))
    before = len(data.get("signal_tracking", []))
    data["signal_tracking"] = []
    data["backtest_stats"] = {}
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{path}: cleared {before} records → 0")

print("Done.")
