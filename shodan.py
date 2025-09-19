#!/usr/bin/env python3
import requests
import csv
import json
from pathlib import Path

URL = "https://internetdb.shodan.io/185.104.29.22"
OUTFILE = Path("output.csv")


def fetch_json(url: str) -> dict:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def list_to_string(value, sep=";"):
    """Convert lists to plain strings, scalars to str, None to ''."""
    if isinstance(value, (list, tuple)):
        return sep.join(str(v) for v in value)
    if value is None:
        return ""
    return str(value)


def save_to_csv(data: dict, out_path: Path):
    # Flatten dict: convert lists to joined strings
    flat = {k: list_to_string(v) for k, v in data.items()}

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flat.keys())
        writer.writeheader()
        writer.writerow(flat)

    print(f"Wrote CSV to {out_path}")


def main():
    data = fetch_json(URL)
    save_to_csv(data, OUTFILE)


if __name__ == "__main__":
    main()
