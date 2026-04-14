"""
Merge all transformed records into the final output files.

Produces:
- output/combined/services.csv — the full merged dataset as CSV
- output/combined/services.json — the full merged dataset as JSON
- output/combined/services.db — the full merged dataset as SQLite
- output/combined/SOURCES.md — attribution for all data sources
- output/gov/{state}/{source}.csv — per-source files
- output/gov/{state}/SOURCES.md — per-state attribution
- output/osm/{source}.csv — OpenStreetMap data
"""

import csv
import json
import os
import sqlite3
from collections import defaultdict
from datetime import date
from config import SOURCES, SCHEMA_FIELDS
from transform import main as transform_all


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# Map jurisdiction codes to folder names
JURISDICTION_FOLDERS = {
    "VIC": "vic",
    "NSW": "nsw",
    "QLD": "qld",
    "SA": "sa",
    "WA": "wa",
    "TAS": "tas",
    "NT": "nt",
    "ACT": "act",
    "federal": "federal",
}


def write_csv(records, path):
    """Write records to a CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        writer.writeheader()
        for record in records:
            row = {field: record.get(field, "") for field in SCHEMA_FIELDS}
            writer.writerow(row)
    print(f"  Written: {path} ({len(records)} records)")


def write_json(records, path):
    """Write records to a JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    output = []
    for record in records:
        row = {field: record.get(field, "") for field in SCHEMA_FIELDS}
        output.append(row)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  Written: {path} ({len(records)} records)")


def write_sqlite(records, path):
    """Write records to a SQLite database."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE services (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            address TEXT,
            suburb TEXT,
            state TEXT,
            postcode TEXT,
            latitude REAL,
            longitude REAL,
            phone TEXT,
            email TEXT,
            website TEXT,
            hours TEXT,
            eligibility TEXT,
            cost TEXT,
            source_id TEXT,
            source_name TEXT,
            source_organisation TEXT,
            source_jurisdiction TEXT,
            source_license TEXT,
            source_url TEXT,
            source_date TEXT,
            quality TEXT
        )
    """)

    # Create indexes for common queries
    cursor.execute("CREATE INDEX idx_state ON services(state)")
    cursor.execute("CREATE INDEX idx_category ON services(category)")
    cursor.execute("CREATE INDEX idx_quality ON services(quality)")
    cursor.execute("CREATE INDEX idx_suburb ON services(suburb)")
    cursor.execute("CREATE INDEX idx_postcode ON services(postcode)")

    for record in records:
        row = {field: record.get(field, "") for field in SCHEMA_FIELDS}
        # Convert lat/lng to real numbers for spatial queries
        for field in ("latitude", "longitude"):
            try:
                row[field] = float(row[field]) if row[field] else None
            except ValueError:
                row[field] = None

        cursor.execute(
            f"INSERT INTO services ({', '.join(SCHEMA_FIELDS)}) VALUES ({', '.join('?' * len(SCHEMA_FIELDS))})",
            [row[f] for f in SCHEMA_FIELDS],
        )

    conn.commit()
    conn.close()
    print(f"  Written: {path} ({len(records)} records)")


def write_sources(sources, path):
    """Write attribution file for given sources."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    today = date.today().strftime("%d %B %Y")

    lines = [
        "# Data Sources and Attribution",
        "",
        "Data sourced from Australian government open data portals.",
        "Used under their respective Creative Commons licenses.",
        "",
        "## Attribution",
        "",
    ]

    for source in sources:
        lines.append(f"- **{source['organisation']}**, {source['jurisdiction']}, "
                      f"*{source['name']}*, Sourced on {today}, "
                      f"[Dataset URL]({source['dataset_url']}). "
                      f"License: {source['license']}")

    lines.append("")
    lines.append(f"Last updated: {date.today().isoformat()}")
    lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Written: {path}")


def write_per_source(all_records):
    """Write individual CSV files per source, organised by state."""
    # Group records by source_id
    by_source = defaultdict(list)
    for record in all_records:
        by_source[record["source_id"]].append(record)

    # Group sources by jurisdiction for SOURCES.md
    sources_by_jurisdiction = defaultdict(list)
    for source in SOURCES:
        sources_by_jurisdiction[source["jurisdiction"]].append(source)

    # Write per-source CSVs
    for source in SOURCES:
        source_records = by_source.get(source["id"], [])
        if not source_records:
            continue

        # OSM goes to osm/ folder, everything else to gov/{state}/
        if source["id"].startswith("osm_"):
            filename = source["id"]
            path = os.path.join(OUTPUT_DIR, "osm", f"{filename}.csv")
            write_csv(source_records, path)
        else:
            folder = JURISDICTION_FOLDERS.get(source["jurisdiction"], "other")
            filename = source["id"].split("_", 1)[-1] if "_" in source["id"] else source["id"]
            path = os.path.join(OUTPUT_DIR, "gov", folder, f"{filename}.csv")
            write_csv(source_records, path)

    # Write per-state SOURCES.md for gov sources
    gov_sources_by_jurisdiction = defaultdict(list)
    osm_sources = []
    for source in SOURCES:
        if source["id"].startswith("osm_"):
            osm_sources.append(source)
        else:
            gov_sources_by_jurisdiction[source["jurisdiction"]].append(source)

    for jurisdiction, sources_list in gov_sources_by_jurisdiction.items():
        folder = JURISDICTION_FOLDERS.get(jurisdiction, "other")
        path = os.path.join(OUTPUT_DIR, "gov", folder, "SOURCES.md")
        write_sources(sources_list, path)

    # Write OSM SOURCES.md
    if osm_sources:
        write_sources(osm_sources, os.path.join(OUTPUT_DIR, "osm", "SOURCES.md"))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Running transform...")
    records = transform_all()

    if not records:
        print("No records to output.")
        return

    print(f"\nWriting per-source files...")
    write_per_source(records)

    print(f"\nWriting combined output ({len(records)} total records)...")
    write_csv(records, os.path.join(OUTPUT_DIR, "combined", "services.csv"))
    write_json(records, os.path.join(OUTPUT_DIR, "combined", "services.json"))
    write_sqlite(records, os.path.join(OUTPUT_DIR, "combined", "services.db"))
    write_sources(SOURCES, os.path.join(OUTPUT_DIR, "combined", "SOURCES.md"))

    print("\nDone.")


if __name__ == "__main__":
    main()
