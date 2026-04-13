"""
Fetch raw data from Australian government open data portals.

Downloads CSV files from each configured source and saves them
to the sources/ directory. Each file is saved with the source ID
as the filename.
"""

import os
import sys
import requests
from config import SOURCES

SOURCES_DIR = os.path.join(os.path.dirname(__file__), "sources")
TIMEOUT = 30
HEADERS = {
    "User-Agent": "commons-au/1.0 (https://github.com/commons-au; open data project)",
}


def fetch_source(source):
    """Download a single data source and save to sources/ directory."""
    source_id = source["id"]
    url = source["url"]
    output_path = os.path.join(SOURCES_DIR, f"{source_id}.csv")

    print(f"Fetching: {source['name']} ({source['organisation']})...")
    print(f"  URL: {url}")

    try:
        response = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: Failed to fetch {source_id}: {e}")
        return False

    # Normalise line endings (\r\n or \r to \n)
    text = response.text.replace("\r\n", "\n").replace("\r", "\n")

    # Check for empty or near-empty responses
    if len(text.strip()) < 10:
        print(f"  WARNING: Empty or near-empty response for {source_id} ({len(text)} bytes)")
        return False

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(text)

    line_count = text.count("\n")
    print(f"  Saved: {output_path} ({line_count} lines)")
    return True


def main():
    os.makedirs(SOURCES_DIR, exist_ok=True)

    success_count = 0
    fail_count = 0

    for source in SOURCES:
        if fetch_source(source):
            success_count += 1
        else:
            fail_count += 1

    print(f"\nDone. {success_count} sources fetched, {fail_count} failed.")

    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
