#!/usr/bin/env python3
"""
run.py – Session 5: Start the Sensor REST API
==============================================
Usage:
    python run.py
    python run.py --port 8000
    python run.py --no-debug
"""
import argparse
import os
import sys

# Allow running from project root: python run.py
sys.path.insert(0, os.path.dirname(__file__))

from sensor_api.app import app

_default_lake = (
    "C:/tmp/datalake/curated/domain=iot" if os.name == "nt"
    else "/tmp/datalake/curated/domain=iot"
)


def main():
    p = argparse.ArgumentParser(description="Session 5 Sensor API")
    p.add_argument("--host",     default="0.0.0.0",  help="Bind host (default 0.0.0.0)")
    p.add_argument("--port",     type=int, default=5000, help="Port (default 5000)")
    p.add_argument("--no-debug", action="store_true",   help="Disable debug mode")
    p.add_argument(
        "--lake-path", default=_default_lake,
        help="Path to the curated Parquet zone from Session 4"
    )
    args = p.parse_args()

    # Allow overriding the data lake path via CLI
    os.environ["CURATED_PATH"] = args.lake_path

    print("=" * 60)
    print(" Session 5 – Sensor Data REST API")
    print(f" Host      : {args.host}:{args.port}")
    print(f" Lake path : {args.lake_path}")
    print(f" Debug     : {not args.no_debug}")
    print("=" * 60)
    print()
    print(" Endpoints:")
    print("   GET  /api/v1/health")
    print("   GET  /api/v1/sensors")
    print("   GET  /api/v1/sensors/<type>/latest")
    print("   GET  /api/v1/sensors/<type>/stats?days=N")
    print("   POST /api/v1/readings")
    print()
    print(" Test with:")
    print(f"   curl -s http://localhost:{args.port}/api/v1/health | python3 -m json.tool")
    print()

    app.run(
        host=args.host,
        port=args.port,
        debug=not args.no_debug,
    )


if __name__ == "__main__":
    main()
