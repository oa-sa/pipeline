"""
Fetch raw data from Australian government open data portals.

Downloads CSV files from each configured source and saves them
to the sources/ directory. Each file is saved with the source ID
as the filename.

Supports two fetch methods:
- "csv" (default): Direct CSV download
- "ckan_datastore": Uses CKAN datastore API (for portals that block direct downloads)
"""

import csv
import io
import os
import sys
import requests
from config import SOURCES

SOURCES_DIR = os.path.join(os.path.dirname(__file__), "sources")
TIMEOUT = 30
HEADERS = {
    "User-Agent": "commons-au/1.0 (https://github.com/commons-au; open data project)",
}


def fetch_csv(source):
    """Fetch via direct CSV download."""
    response = requests.get(source["url"], timeout=TIMEOUT, headers=HEADERS)
    response.raise_for_status()
    text = response.text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text.strip()) < 10:
        return None
    return text


def fetch_ckan_datastore(source):
    """Fetch via CKAN datastore API and convert to CSV."""
    resource_id = source["ckan_resource_id"]
    api_base = source["ckan_api_base"]

    # First get total count
    r = requests.get(
        f"{api_base}datastore_search",
        params={"resource_id": resource_id, "limit": 0},
        timeout=TIMEOUT,
        headers=HEADERS,
    )
    r.raise_for_status()
    total = r.json()["result"]["total"]
    fields = [f["id"] for f in r.json()["result"]["fields"] if f["id"] != "_id"]

    # Fetch all records (CKAN default limit is 100, so paginate)
    all_records = []
    offset = 0
    batch_size = 500
    while offset < total:
        r = requests.get(
            f"{api_base}datastore_search",
            params={"resource_id": resource_id, "limit": batch_size, "offset": offset},
            timeout=TIMEOUT,
            headers=HEADERS,
        )
        r.raise_for_status()
        records = r.json()["result"]["records"]
        all_records.extend(records)
        offset += batch_size

    # Convert to CSV string
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for record in all_records:
        writer.writerow(record)

    return output.getvalue()


def fetch_source(source):
    """Download a single data source and save to sources/ directory."""
    source_id = source["id"]
    output_path = os.path.join(SOURCES_DIR, f"{source_id}.csv")
    method = source.get("fetch_method", "csv")

    print(f"Fetching: {source['name']} ({source['organisation']})...")
    print(f"  Method: {method}")

    try:
        if method == "ckan_datastore":
            text = fetch_ckan_datastore(source)
        else:
            text = fetch_csv(source)
    except requests.RequestException as e:
        print(f"  ERROR: Failed to fetch {source_id}: {e}")
        return False

    if not text:
        print(f"  WARNING: Empty or near-empty response for {source_id}")
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
