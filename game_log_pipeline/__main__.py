import argparse
import json
import urllib.request
from pathlib import Path
from .generator import events
from .pipeline import canonical, connect, ingest, rebuild, report


def local(args):
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


def send(args):
    """Post the synthetic fixture to a running ingest API (used by the in-cluster generator Job)."""
    body = ("\n".join(canonical(item) for item in events(args.seed)) + "\n").encode("utf-8")
    request = urllib.request.Request(args.url.rstrip("/") + "/v1/events", data=body,
                                     headers={"Content-Type": "application/x-ndjson"}, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        print(response.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Synthetic idle reward integrity pipeline")
    parser.add_argument("command", choices=["demo", "replay", "api", "load", "rebuild", "send"])
    parser.add_argument("--output", default="runs/demo")
    parser.add_argument("--input", help="JSONL input required by replay")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--url", default="http://localhost:8080", help="ingest API base URL for send")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                        help="rebuild: recompute only what changed since each target's watermark, or everything")
    args = parser.parse_args()
    if args.command == "replay" and not args.input:
        parser.error("replay requires --input")
    if args.command in ("demo", "replay"):
        local(args)
    elif args.command == "send":
        send(args)
    elif args.command == "api":
        import uvicorn
        from .services import create_app
        uvicorn.run(create_app(), host="0.0.0.0", port=args.port)
    elif args.command == "load":
        from .services import run_loader
        run_loader()
    else:
        from .services import run_rebuild
        run_rebuild(args.mode)


if __name__ == "__main__":
    main()
