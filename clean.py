"""
Field-level cleaners applied during transform.

Each function takes a raw string and returns a normalised string.
Empty/sentinel input returns "".
"""

import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


SENTINELS = {"", "(blank)", "n/a", "na", "none", "-", "--", "null", "nil"}


def _is_sentinel(value):
    return value is None or value.strip().lower() in SENTINELS


# ---------------------------------------------------------------------------
# Phone
# ---------------------------------------------------------------------------

# Short codes that should be preserved as-is (without +61)
_SHORT_CODES = {
    "000", "106", "112",
    "131444", "132500", "132848", "131114", "131450", "131611",
}


def normalise_phone(value):
    """
    Normalise an Australian phone number.

    - Landlines and mobiles: +61 X XXXX XXXX
    - 13/1300/1800 numbers: kept as national short-form (no +61 prefix)
    - Emergency/short codes: kept as-is
    - Invalid/garbage: returns ""
    """
    if _is_sentinel(value):
        return ""

    raw = value.strip()

    # Take the first phone if multiple are present (comma, slash, semicolon, " or ", " / ")
    first = re.split(r"\s*(?:,|;|/| or )\s*", raw, maxsplit=1)[0]

    # Strip everything except digits and leading +
    has_plus = first.strip().startswith("+")
    digits = re.sub(r"\D", "", first)

    if not digits:
        return ""

    # Handle international prefix: 0061... or +61...
    if has_plus and digits.startswith("61"):
        digits = digits[2:]
    elif digits.startswith("0061"):
        digits = digits[4:]
    elif digits.startswith("61") and len(digits) in (11, 12):
        # Ambiguous but common: 61XXXXXXXXX — strip country code
        digits = digits[2:]

    # Strip a single leading 0 (national trunk prefix)
    if digits.startswith("0") and len(digits) in (10, 11):
        digits = digits[1:]

    # 13/1300/1800 numbers
    if digits.startswith("13") and len(digits) in (6, 10):
        # 13 XXXX or 1300/1800 XXX XXX
        if len(digits) == 6:
            return f"{digits[:2]} {digits[2:]}"
        return f"{digits[:4]} {digits[4:7]} {digits[7:]}"
    if digits.startswith("18") and len(digits) == 10:
        return f"{digits[:4]} {digits[4:7]} {digits[7:]}"

    # Short emergency codes
    if digits in _SHORT_CODES:
        return digits

    # Mobile (4XX XXX XXX) or landline (area code X + 8 digits)
    if len(digits) == 9:
        if digits.startswith("4"):
            return f"+61 {digits[0]} {digits[1:5]} {digits[5:]}"
        # Landline: area code is first digit (2,3,7,8)
        if digits[0] in ("2", "3", "7", "8"):
            return f"+61 {digits[0]} {digits[1:5]} {digits[5:]}"

    # Couldn't confidently normalise — return empty rather than keep garbage
    return ""


# ---------------------------------------------------------------------------
# Website
# ---------------------------------------------------------------------------

_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "gclid", "fbclid", "mc_cid", "mc_eid", "ref", "referrer",
}


def normalise_website(value):
    """
    Normalise a website URL.

    - Strips surrounding whitespace/HTML
    - Adds https:// if scheme is missing
    - Lowercases host
    - Removes tracking query params
    - Removes trailing slash from path root
    - Returns "" for mailto:, tel:, obviously bad input
    """
    if _is_sentinel(value):
        return ""

    raw = value.strip().strip("<>\"'")
    if not raw:
        return ""

    # Reject non-http schemes
    low = raw.lower()
    if low.startswith(("mailto:", "tel:", "fax:", "javascript:")):
        return ""

    # Add scheme if missing
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        raw = "https://" + raw

    try:
        parts = urlsplit(raw)
    except ValueError:
        return ""

    if not parts.netloc or "." not in parts.netloc:
        return ""

    host = parts.netloc.lower()
    # Strip default ports
    if host.endswith(":80") or host.endswith(":443"):
        host = host.rsplit(":", 1)[0]

    # Scheme: keep https unless original was explicitly http
    scheme = "http" if parts.scheme.lower() == "http" else "https"

    # Clean query: drop tracking params
    query_pairs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                   if k.lower() not in _TRACKING_PARAMS]
    query = urlencode(query_pairs)

    # Normalise path: drop trailing slash unless path is "/"
    path = parts.path or ""
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")

    return urlunsplit((scheme, host, path, query, ""))


# ---------------------------------------------------------------------------
# Suburb
# ---------------------------------------------------------------------------

# Words that should stay lowercase in multi-word suburb names
_SUBURB_LOWER = {"of", "the", "on", "upon", "and"}

# Particles that need special casing
_PARTICLE_PATTERNS = [
    (re.compile(r"\bMc([a-z])"), lambda m: "Mc" + m.group(1).upper()),
    (re.compile(r"\bMac([a-z]{3,})"), lambda m: "Mac" + m.group(1).capitalize()),
    (re.compile(r"\bO'([a-z])"), lambda m: "O'" + m.group(1).upper()),
    (re.compile(r"\bSt\.?\s+([a-z])"), lambda m: "St " + m.group(1).upper()),
    (re.compile(r"\bMt\.?\s+([a-z])"), lambda m: "Mt " + m.group(1).upper()),
]


def normalise_suburb(value):
    """
    Title-case a suburb name while handling Mc/Mac/O'/St/Mt particles
    and keeping short joining words lowercase.
    """
    if _is_sentinel(value):
        return ""

    s = value.strip()
    if not s:
        return ""

    # Collapse internal whitespace
    s = re.sub(r"\s+", " ", s)

    # If mixed case already (not ALLCAPS, not all lower), assume the source
    # knew what it was doing and just fix whitespace.
    if not s.isupper() and not s.islower():
        return s

    # Title-case word by word, keeping joining words lowercase except first
    words = s.split(" ")
    out = []
    for i, word in enumerate(words):
        if not word:
            continue
        lw = word.lower()
        if i > 0 and lw in _SUBURB_LOWER:
            out.append(lw)
        elif "-" in word:
            out.append("-".join(p.capitalize() for p in word.split("-")))
        else:
            out.append(word.capitalize())
    result = " ".join(out)

    # Apply particle fixes
    for pat, repl in _PARTICLE_PATTERNS:
        result = pat.sub(repl, result)

    return result
