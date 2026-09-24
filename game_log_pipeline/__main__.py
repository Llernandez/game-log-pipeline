import argparse
import json
from pathlib import Path
from .generator import events
from .pipeline import canonical, connect, ingest, rebuild, report

def main():
    parser = argparse.ArgumentParser(description="Synthetic idle reward integrity demo")
    parser.add_argument("command", choices=["demo", "replay"])
    parser.add_argument("--output", default="runs/demo")
    parser.add_argument("--input", help="JSONL input required by replay")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.command == "replay" and not args.input:
        parser.error("replay requires --input")
    target = Path(args.output)
    target.mkdir(parents=True, exist_ok=True)
    source = Path(args.input) if args.command == "replay" else target / "events.jsonl"
    if args.command == "demo":
        source.write_text("\n".join(canonical(item) for item in events(args.seed)) + "\n", encoding="utf-8")
    db = connect(target / "pipeline.sqlite")
    try:
        with source.open(encoding="utf-8") as stream:
            ingest(db, stream)
        rebuild(db)
        result = report(db)
        output = json.dumps(result, ensure_ascii=False, indent=2)
        (target / "report.json").write_text(output + "\n", encoding="utf-8")
        print(output)
    finally:
        db.close()

if __name__ == "__main__":
    main()
