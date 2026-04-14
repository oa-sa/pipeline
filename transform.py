"""
Transform raw source data into the oa-sa standard schema.

Reads CSVs from sources/, applies field mappings and cleaning,
and outputs standardised records.
"""

import csv
import os
import re
from datetime import date
from config import SOURCES, CATEGORIES


SOURCES_DIR = os.path.join(os.path.dirname(__file__), "sources")


def clean_text(value):
    """Clean whitespace and normalise a text field."""
    if not value:
        return ""
    return " ".join(value.strip().split())


def strip_html(value):
    """Remove HTML tags from a string."""
    if not value:
        return ""
    return re.sub(r"<[^>]+>", "", value).strip()


STATE_NAME_MAP = {
    "new south wales": "NSW",
    "victoria": "VIC",
    "queensland": "QLD",
    "south australia": "SA",
    "western australia": "WA",
    "tasmania": "TAS",
    "northern territory": "NT",
    "australian capital territory": "ACT",
    "qld": "QLD",
    "nsw": "NSW",
    "vic": "VIC",
    "sa": "SA",
    "wa": "WA",
    "tas": "TAS",
    "nt": "NT",
    "act": "ACT",
    "(blank)": "",
}

# Australian postcode ranges to state mapping
POSTCODE_STATE_MAP = [
    (200, 299, "ACT"),
    (800, 899, "NT"),
    (900, 999, "NT"),
    (1000, 2599, "NSW"),
    (2600, 2619, "ACT"),
    (2620, 2899, "NSW"),
    (2900, 2920, "ACT"),
    (2921, 2999, "NSW"),
    (3000, 3999, "VIC"),
    (4000, 4999, "QLD"),
    (5000, 5799, "SA"),
    (5800, 5999, "SA"),
    (6000, 6797, "WA"),
    (6800, 6999, "WA"),
    (7000, 7799, "TAS"),
    (7800, 7999, "TAS"),
]


def normalise_state(state):
    """Normalise state to 2-3 letter code."""
    if not state:
        return ""
    return STATE_NAME_MAP.get(state.strip().lower(), state.strip().upper())


def state_from_postcode(postcode):
    """Derive state from Australian postcode."""
    if not postcode:
        return ""
    try:
        pc = int(postcode.strip()[:4])
        for low, high, state in POSTCODE_STATE_MAP:
            if low <= pc <= high:
                return state
    except (ValueError, IndexError):
        pass
    return ""


def extract_state_from_address(address):
    """Try to extract Australian state from an address string."""
    states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
    upper = address.upper()
    for state in states:
        if f" {state} " in upper or upper.endswith(f" {state}"):
            return state
    return ""


def map_category(raw_categories):
    """Map source category labels to our standard categories."""
    for raw in raw_categories:
        raw_lower = raw.strip().lower()
        if raw_lower in CATEGORIES:
            return CATEGORIES[raw_lower]
    return "other"


def build_hours_melbourne(row, field_map):
    """Build hours string from Melbourne's per-day columns."""
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    day_abbrevs = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    parts = []
    for day, abbrev in zip(days, day_abbrevs):
        key = f"hours_{day}"
        if key in field_map:
            col = field_map[key]
            value = clean_text(row.get(col, ""))
            if value and value.lower() not in ("closed", "n/a", ""):
                parts.append(f"{abbrev}: {value}")
    return "; ".join(parts)


def build_hours_casey(row, field_map):
    """Build hours string from Casey's hours columns."""
    parts = []
    for key in ["hours_1", "hours_2"]:
        if key in field_map:
            col = field_map[key]
            value = clean_text(row.get(col, ""))
            if value:
                parts.append(value)
    return "; ".join(parts)


def transform_melbourne(row, source):
    """Transform a City of Melbourne record."""
    field_map = source["field_map"]

    # Build categories from the multiple category columns
    raw_categories = []
    for key in ["category_1", "category_2", "category_3", "category_4", "category_5", "category_6"]:
        if key in field_map:
            val = clean_text(row.get(field_map[key], ""))
            if val and val.upper() != "N/A":
                raw_categories.append(val)

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": clean_text(row.get(field_map.get("description", ""), "")),
        "category": map_category(raw_categories),
        "address": clean_text(row.get(field_map.get("address", ""), "")),
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": source["jurisdiction"],
        "postcode": "",
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": build_hours_melbourne(row, field_map),
        "eligibility": "",
        "cost": clean_text(row.get(field_map.get("cost", ""), "")),
    }


def transform_casey(row, source):
    """Transform a City of Casey record."""
    field_map = source["field_map"]

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": clean_text(row.get(field_map.get("description", ""), "")),
        "category": "food",
        "address": clean_text(row.get(field_map.get("address", ""), "")),
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": source["jurisdiction"],
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": build_hours_casey(row, field_map),
        "eligibility": "",
        "cost": "Free",
    }


def transform_emergency_relief(row, source):
    """Transform a federal Emergency Relief Provider Outlets record."""
    field_map = source["field_map"]

    # Website field contains HTML tags like <a href='...'>...</a>
    raw_website = row.get(field_map.get("website", ""), "")
    website = strip_html(raw_website)

    # Category from the activity name
    raw_category = clean_text(row.get(field_map.get("category_raw", ""), ""))
    category = map_category([raw_category]) if raw_category else "financial"

    # Address contains full address with state — extract state
    raw_address = clean_text(row.get(field_map.get("address", ""), ""))
    state = extract_state_from_address(raw_address)

    # Build description from org name
    org_name = clean_text(row.get(field_map.get("organisation_name", ""), ""))
    name = clean_text(row.get(field_map.get("name", ""), ""))
    description = f"Emergency relief provider operated by {org_name}" if org_name else "Emergency relief provider"

    return {
        "name": name if name else org_name,
        "description": description,
        "category": category,
        "address": raw_address,
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": state,
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": "",
        "email": "",
        "website": website,
        "hours": "",
        "eligibility": "",
        "cost": "Free",
    }


def transform_employment_services(row, source):
    """Transform a federal Employment Services Provider record."""
    field_map = source["field_map"]

    name = clean_text(row.get(field_map.get("name", ""), ""))
    location = clean_text(row.get(field_map.get("location_name", ""), ""))

    # Use phone or freecall
    phone = clean_text(row.get(field_map.get("phone", ""), ""))
    freecall = clean_text(row.get(field_map.get("freecall", ""), ""))
    phone = freecall if freecall and freecall.strip() else phone

    description = f"Employment services provider in {location}" if location else "Employment services provider"

    return {
        "name": name,
        "description": description,
        "category": "employment",
        "address": clean_text(row.get(field_map.get("address", ""), "")),
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": clean_text(row.get(field_map.get("state", ""), "")),
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": phone,
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": "",
        "eligibility": "",
        "cost": "Free",
    }


def transform_generic(row, source, default_category, default_description=""):
    """Generic transform that works for simple datasets with standard field names."""
    field_map = source["field_map"]

    raw_category = clean_text(row.get(field_map.get("category_raw", ""), ""))
    category = map_category([raw_category]) if raw_category else default_category

    address = clean_text(row.get(field_map.get("address", ""), ""))
    address_2 = clean_text(row.get(field_map.get("address_2", ""), ""))
    if address_2:
        address = f"{address}, {address_2}"

    description = clean_text(row.get(field_map.get("description", ""), ""))
    services = clean_text(row.get(field_map.get("services", ""), ""))
    if not description:
        description = services if services else default_description

    state = clean_text(row.get(field_map.get("state", ""), ""))
    if not state:
        state = source["jurisdiction"]

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": description,
        "category": category,
        "address": address,
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": state,
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": clean_text(row.get(field_map.get("hours", ""), "")),
        "eligibility": clean_text(row.get(field_map.get("eligibility", ""), "")),
        "cost": "Free",
    }


def transform_sa_community(row, source):
    """Transform a SA Community Directory record."""
    field_map = source["field_map"]

    raw_category = clean_text(row.get(field_map.get("category_raw", ""), ""))
    category = map_category([raw_category]) if raw_category else "community"

    tag_line = clean_text(row.get(field_map.get("tag_line", ""), ""))
    services = clean_text(row.get(field_map.get("services", ""), ""))
    description = tag_line if tag_line else services

    # SA has full state name
    state = clean_text(row.get(field_map.get("state", ""), ""))
    if state.lower() == "south australia":
        state = "SA"

    address = clean_text(row.get(field_map.get("address", ""), ""))
    address_2 = clean_text(row.get(field_map.get("address_2", ""), ""))
    if address_2:
        address = f"{address}, {address_2}"

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": description,
        "category": category,
        "address": address,
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": state,
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": clean_text(row.get(field_map.get("hours", ""), "")),
        "eligibility": clean_text(row.get(field_map.get("eligibility", ""), "")),
        "cost": clean_text(row.get(field_map.get("fees", ""), "")),
    }


def transform_qld_standard(row, source, default_category="information"):
    """Generic transform for QLD datasets that share a similar column structure."""
    field_map = source["field_map"]

    raw_category = clean_text(row.get(field_map.get("category_raw", ""), ""))
    category = map_category([raw_category]) if raw_category else default_category

    address = clean_text(row.get(field_map.get("address", ""), ""))
    address_2 = clean_text(row.get(field_map.get("address_2", ""), ""))
    if address_2:
        address = f"{address}, {address_2}"

    description = clean_text(row.get(field_map.get("description", ""), ""))
    services = clean_text(row.get(field_map.get("services", ""), ""))
    if not description and services:
        description = services

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": description,
        "category": category,
        "address": address,
        "suburb": clean_text(row.get(field_map.get("suburb", ""), "")),
        "state": clean_text(row.get(field_map.get("state", ""), "QLD")),
        "postcode": clean_text(row.get(field_map.get("postcode", ""), "")),
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": clean_text(row.get(field_map.get("email", ""), "")),
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": clean_text(row.get(field_map.get("hours", ""), "")),
        "eligibility": "",
        "cost": "Free",
    }


def transform_qld_gov_counters(row, source):
    return transform_qld_standard(row, source, default_category="information")


def transform_qld_housing(row, source):
    return transform_qld_standard(row, source, default_category="housing")


def transform_qld_breastscreen(row, source):
    return transform_qld_standard(row, source, default_category="health")


def transform_qld_dispute(row, source):
    return transform_qld_standard(row, source, default_category="legal")


def transform_qld_victim_support(row, source):
    """Transform QLD victim support — no address fields, has region instead."""
    field_map = source["field_map"]

    raw_category = clean_text(row.get(field_map.get("category_raw", ""), ""))
    category = map_category([raw_category]) if raw_category else "legal"

    description = clean_text(row.get(field_map.get("description", ""), ""))
    audience = clean_text(row.get(field_map.get("audience", ""), ""))
    services = clean_text(row.get(field_map.get("services", ""), ""))

    return {
        "name": clean_text(row.get(field_map.get("name", ""), "")),
        "description": description,
        "category": category,
        "address": "",
        "suburb": "",
        "state": "QLD",
        "postcode": "",
        "latitude": "",
        "longitude": "",
        "phone": clean_text(row.get(field_map.get("phone", ""), "")),
        "email": "",
        "website": clean_text(row.get(field_map.get("website", ""), "")),
        "hours": "",
        "eligibility": audience,
        "cost": "Free",
    }


def transform_tas_service(row, source):
    """Transform TAS Service Tasmania shops."""
    field_map = source["field_map"]

    # Address contains full address with state
    raw_address = clean_text(row.get(field_map.get("address", ""), ""))

    return {
        "name": f"Service Tasmania - {clean_text(row.get(field_map.get('name', ''), ''))}",
        "description": "Service Tasmania government service shop",
        "category": "information",
        "address": raw_address,
        "suburb": "",
        "state": "TAS",
        "postcode": "",
        "latitude": clean_text(row.get(field_map.get("latitude", ""), "")),
        "longitude": clean_text(row.get(field_map.get("longitude", ""), "")),
        "phone": "",
        "email": "",
        "website": "",
        "hours": clean_text(row.get(field_map.get("hours", ""), "")),
        "eligibility": "",
        "cost": "Free",
    }


# Map source IDs to their transform functions
TRANSFORMERS = {
    "vic_melbourne_helping_out": transform_melbourne,
    "vic_casey_food_relief": transform_casey,
    "fed_emergency_relief": transform_emergency_relief,
    "fed_employment_services": transform_employment_services,
    "fed_judicial_courts": lambda row, src: transform_generic(row, src, "legal", "Court"),
    "sa_community_directory": transform_sa_community,
    "sa_child_family_health": lambda row, src: transform_generic(row, src, "health", "Child and family health centre"),
    "qld_gov_service_counters": transform_qld_gov_counters,
    "qld_housing_centres": transform_qld_housing,
    "qld_breastscreen": transform_qld_breastscreen,
    "qld_victim_support": transform_qld_victim_support,
    "qld_dispute_resolution": transform_qld_dispute,
    "qld_contacts_dccsds": lambda row, src: transform_qld_standard(row, src, "community"),
    "qld_youth_justice_centres": lambda row, src: transform_qld_standard(row, src, "legal"),
    "qld_housing_finder": lambda row, src: transform_qld_standard(row, src, "housing"),
    "qld_hep_c_centres": lambda row, src: transform_qld_standard(row, src, "health"),
    "tas_service_shops": transform_tas_service,
    "vic_ballarat_food": lambda row, src: transform_generic(row, src, "food", "Community food activity"),
    "vic_ballarat_community_centres": lambda row, src: transform_generic(row, src, "community", "Community centre/hall"),
    "vic_neighbourhood_houses": lambda row, src: transform_generic(row, src, "community", "Neighbourhood house"),
    "vic_casey_libraries": lambda row, src: transform_generic(row, src, "community", "Library"),
    "vic_casey_maternal_health": lambda row, src: transform_generic(row, src, "health", "Maternal and child health centre"),
    "vic_ballarat_kindergartens": lambda row, src: transform_generic(row, src, "education", "Kindergarten"),
    "vic_ballarat_early_learning": lambda row, src: transform_generic(row, src, "education", "Early learning centre"),
    "sa_gp_plus": lambda row, src: transform_generic(row, src, "health", "GP Plus health clinic"),
    "sa_private_hospitals": lambda row, src: transform_generic(row, src, "health", "Private hospital"),
}


def transform_source(source):
    """Transform all records from a single source."""
    source_id = source["id"]
    source_path = os.path.join(SOURCES_DIR, f"{source_id}.csv")

    if not os.path.exists(source_path):
        print(f"  WARNING: Source file not found: {source_path}")
        return []

    transformer = TRANSFORMERS.get(source_id)
    if not transformer:
        print(f"  WARNING: No transformer for source: {source_id}")
        return []

    records = []
    today = date.today().isoformat()

    with open(source_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            record = transformer(row, source)

            # Skip records with no name
            if not record["name"]:
                continue

            # Normalise state
            record["state"] = normalise_state(record.get("state", ""))

            # If state is still empty, try to derive from postcode
            if not record["state"] and record.get("postcode"):
                record["state"] = state_from_postcode(record["postcode"])

            # If still empty, try to extract from address
            if not record["state"] and record.get("address"):
                record["state"] = extract_state_from_address(record["address"])

            # Score record quality
            has_location = bool(record.get("address", "").strip() or record.get("latitude", "").strip())
            has_contact = bool(record.get("phone", "").strip() or record.get("website", "").strip() or record.get("email", "").strip())
            has_description = bool(record.get("description", "").strip())
            has_state = bool(record.get("state", "").strip())

            if has_location and has_contact:
                record["quality"] = "complete"
            elif has_location or has_contact:
                record["quality"] = "partial"
            else:
                record["quality"] = "minimal"

            # Add source metadata and ID
            record["id"] = f"{source_id}_{i:04d}"
            record["source_id"] = source_id
            record["source_name"] = source["name"]
            record["source_organisation"] = source["organisation"]
            record["source_jurisdiction"] = source["jurisdiction"]
            record["source_license"] = source["license"]
            record["source_url"] = source["dataset_url"]
            record["source_date"] = today

            records.append(record)

    print(f"  Transformed: {len(records)} records from {source['name']}")
    return records


def main():
    all_records = []

    for source in SOURCES:
        print(f"Transforming: {source['name']}...")
        records = transform_source(source)
        all_records.extend(records)

    print(f"\nTotal: {len(all_records)} records transformed.")
    return all_records


if __name__ == "__main__":
    main()
