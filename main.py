#!/usr/bin/env python3
import argparse
from pathlib import Path
from src.config import load_settings
from src.converter import run

BASE=Path(__file__).resolve().parent

def main():
    ap=argparse.ArgumentParser(description="Standalone Archive Viewer compatible CSV -> Parquet converter")
    ap.add_argument("--config",default=str(BASE/"config/settings.txt"))
    args=ap.parse_args()
    settings=load_settings(args.config)
    run(settings,BASE)

if __name__=="__main__": main()
