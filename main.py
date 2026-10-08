#!/usr/bin/env python3

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.environ["PYTHONPATH"] = str(BASE_DIR) + os.pathsep + os.environ.get("PYTHONPATH", "")

from src.config import load_settings
from src.converter import run


def print_usage():
    print()
    print("CSV -> PARQUET CONVERTER")
    print()
    print("Usage:")
    print("  python3.12 main.py <input_path> <output_path> <datatypes_json> <tika_host>")
    print()
    print("Example:")
    print("  python3.12 main.py /home/postgres/input /home/postgres/output /home/postgres/input/datatypes_36.json http://svcoptim6035:9998")
    print()


def main():
    if len(sys.argv) != 5:
        print_usage()
        sys.exit(1)

    input_path = Path(sys.argv[1]).expanduser().resolve()
    output_path = Path(sys.argv[2]).expanduser().resolve()
    datatype_json = Path(sys.argv[3]).expanduser().resolve()
    tika_host = sys.argv[4]

    if not input_path.exists():
        print(f"ERROR: Input path does not exist:\n  {input_path}")
        sys.exit(1)

    if not datatype_json.is_file():
        print(f"ERROR: Datatype JSON file does not exist:\n  {datatype_json}")
        sys.exit(1)

    output_path.mkdir(parents=True, exist_ok=True)

    try:
        settings = load_settings(BASE_DIR / "config" / "settings.txt")
        settings["INPUT_PATH"] = str(input_path)
        settings["OUTPUT_PATH"] = str(output_path)
        settings["DATATYPE_JSON"] = str(datatype_json)
        settings["TIKA_HOST"] = tika_host

        run(settings, BASE_DIR)

    except KeyboardInterrupt:
        print("\nConversion interrupted by user.")
        sys.exit(130)

    except Exception as exc:
        print(f"\nConversion failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()