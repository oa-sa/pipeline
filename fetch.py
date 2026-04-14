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
import json
import os
import sys
import time
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


def fetch_overpass(source):
    """Fetch from OpenStreetMap via Overpass API, querying state by state."""
    overpass_url = "https://overpass-api.de/api/interpreter"
    tags = source["osm_tags"]

    # Australian state bounding boxes
    state_bboxes = {
        "NSW": (-37.5, 140.9, -28.0, 154.0),
        "VIC": (-39.2, 140.9, -33.9, 150.0),
        "QLD": (-29.2, 137.9, -10.0, 154.0),
        "SA": (-38.1, 129.0, -26.0, 141.0),
        "WA": (-35.2, 112.9, -13.7, 129.0),
        "TAS": (-43.7, 143.8, -39.5, 148.5),
        "NT": (-26.0, 129.0, -10.9, 138.0),
        "ACT": (-35.95, 148.7, -35.1, 149.4),
    }

    all_elements = []
    for state, (s, w, n, e) in state_bboxes.items():
        # Build query for all requested tags in this bbox
        parts = []
        for tag in tags:
            parts.append(f'node[{tag}]({s},{w},{n},{e});')
        query = f'[out:json][timeout:90];({" ".join(parts)});out body;'

        try:
            r = requests.post(overpass_url, data={"data": query}, timeout=120, headers=HEADERS)
            if r.status_code == 200:
                elements = r.json().get("elements", [])
                # Tag each element with the state
                for el in elements:
                    el["_state"] = state
                all_elements.extend(elements)
                print(f"    {state}: {len(elements)} found")
            elif r.status_code == 429:
                print(f"    {state}: Rate limited, waiting 30s...")
                time.sleep(30)
            else:
                print(f"    {state}: HTTP {r.status_code}")
        except requests.RequestException as ex:
            print(f"    {state}: Error — {ex}")

        time.sleep(3)  # Be respectful between states

    if not all_elements:
        return None

    # Convert to CSV
    output = io.StringIO()
    fieldnames = ["osm_id", "name", "lat", "lon", "state", "amenity", "office",
                  "social_facility", "social_facility_for", "addr_street",
                  "addr_suburb", "addr_postcode", "phone", "website", "email",
                  "opening_hours", "wheelchair"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for el in all_elements:
        tags = el.get("tags", {})
        writer.writerow({
            "osm_id": el.get("id", ""),
            "name": tags.get("name", ""),
            "lat": el.get("lat", ""),
            "lon": el.get("lon", ""),
            "state": el.get("_state", ""),
            "amenity": tags.get("amenity", ""),
            "office": tags.get("office", ""),
            "social_facility": tags.get("social_facility", ""),
            "social_facility_for": tags.get("social_facility:for", ""),
            "addr_street": tags.get("addr:street", ""),
            "addr_suburb": tags.get("addr:suburb", ""),
            "addr_postcode": tags.get("addr:postcode", ""),
            "phone": tags.get("phone", tags.get("contact:phone", "")),
            "website": tags.get("website", tags.get("contact:website", "")),
            "email": tags.get("email", tags.get("contact:email", "")),
            "opening_hours": tags.get("opening_hours", ""),
            "wheelchair": tags.get("wheelchair", ""),
        })

    return output.getvalue()


def fetch_source(source):
    """Download a single data source and save to sources/ directory."""
    source_id = source["id"]
    output_path = os.path.join(SOURCES_DIR, f"{source_id}.csv")
    method = source.get("fetch_method", "csv")

    print(f"Fetching: {source['name']} ({source['organisation']})...")
    print(f"  Method: {method}")

    try:
        if method == "overpass":
            text = fetch_overpass(source)
        elif method == "ckan_datastore":
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
    import argparse
    parser = argparse.ArgumentParser(description="Fetch data from configured sources")
    parser.add_argument("--filter", choices=["gov", "osm", "all"], default="all",
                        help="Which sources to fetch: gov (government only), osm (OpenStreetMap only), all (default)")
    args = parser.parse_args()

    os.makedirs(SOURCES_DIR, exist_ok=True)

    sources_to_fetch = []
    for source in SOURCES:
        is_osm = source["id"].startswith("osm_")
        if args.filter == "gov" and is_osm:
            continue
        if args.filter == "osm" and not is_osm:
            continue
        sources_to_fetch.append(source)

    print(f"Fetching {len(sources_to_fetch)} sources (filter: {args.filter})...\n")

    success_count = 0
    fail_count = 0

    for source in sources_to_fetch:
        if fetch_source(source):
            success_count += 1
        else:
            fail_count += 1

    print(f"\nDone. {success_count} sources fetched, {fail_count} failed.")

    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
