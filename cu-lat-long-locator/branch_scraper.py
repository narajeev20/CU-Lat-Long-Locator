"""
Scrapes a credit union website for branch names and addresses,
then geocodes addresses to lat/long.

Branch-to-address matching is scoped: find the branch name node first,
then only search for address within the same local container (or next sibling).
"""
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Full-page address patterns (fallback when address is split across nodes)
ADDRESS_PATTERN = re.compile(
    r"\d{1,6}[\w\s\.\-]*(?:street|st|avenue|ave|blvd|boulevard|road|rd|drive|dr|lane|ln|way|court|ct|place|pl|suite|ste)[\w\s\.\-]*"
    r"[\s,]+[\w\s\.\-]+[\s,]+\b[A-Za-z]{2}\b[\s,]+\d{5}(?:-\d{4})?",
    re.IGNORECASE,
)
SIMPLE_ADDRESS = re.compile(
    r"[\w\s\.\-]{3,80},?\s*[A-Za-z]{2}\s*,?\s*\d{5}(?:-\d{4})?",
    re.IGNORECASE,
)

# ── Patterns ───────────────────────────────────────────────────────────────

# href patterns that indicate a maps link (real URL or local anchor)
_MAPS_HREF = re.compile(
    r"(maps\.app\.goo\.gl|maps\.google\.|goo\.gl/maps|apple\.com/maps"
    r"|bing\.com/maps|mapquest\.com|#.*map)",
    re.IGNORECASE,
)

# Class/id names that strongly indicate an address container
_ADDR_CLASS = re.compile(
    r"(address|location|branch.?info|contact.?info|company_information__location)",
    re.IGNORECASE,
)

# Noise lines we want to discard before scoring
_NOISE = re.compile(
    r"(phone|fax|tel:|toll.?free|hours?|open|closed|"
    r"mon|tue|wed|thu|fri|sat|sun|"
    r"drive.?thru|lobby|directions|watch|webcam|call\s|"
    r"get\s+directions|view\s+map|click\s+here)",
    re.IGNORECASE,
)

# --- Normalize and match (Step 1) ---
def normalize(text: str) -> str:
    """Lowercase, replace non-alphanumeric with spaces, collapse whitespace, trim."""
    if not text:
        return ""
    s = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
    return " ".join(s.split()).strip()


def _tokens(s: str):
    return set(normalize(s).split()) if normalize(s) else set()


# Tags we consider for branch name (heading-like)
BRANCH_HEADING_TAGS = {"h1", "h2", "h3", "h4", "strong", "span", "a", "div", "p"}


def _class_contains_heading_or_hdr(tag) -> bool:
    cls = tag.get("class") or []
    joined = " ".join(cls).lower() if isinstance(cls, list) else str(cls).lower()
    return "heading" in joined or "hdr" in joined


def _get_candidate_text(tag) -> str:
    return (tag.get_text(separator=" ", strip=True) or "").strip()


def _exact_normalized_match(target_norm: str, candidate_norm: str) -> bool:
    return target_norm == candidate_norm


def _fuzzy_match(target_branch_name: str, candidate_text: str, threshold: float = 0.85) -> bool:
    target_tok = _tokens(target_branch_name)
    cand_tok = _tokens(candidate_text)
    if not target_tok:
        return bool(cand_tok)
    if target_tok <= cand_tok:
        return True
    inter = len(target_tok & cand_tok)
    sim = inter / len(target_tok) if target_tok else 0
    return sim >= threshold


def _find_branch_node(soup: BeautifulSoup, target_branch_name: str):
    """Find the DOM element that best matches the branch name (exact then fuzzy)."""
    target_norm = normalize(target_branch_name)
    if not target_norm:
        return None

    candidates = []
    for tag in soup.find_all(True):
        if tag.name not in BRANCH_HEADING_TAGS and not _class_contains_heading_or_hdr(tag):
            continue
        text = _get_candidate_text(tag)
        if not text or len(text) > 200:
            continue
        cand_norm = normalize(text)
        if _exact_normalized_match(target_norm, cand_norm):
            return tag
        if _fuzzy_match(target_branch_name, text):
            candidates.append((tag, text))

    if not candidates:
        return None
    candidates.sort(key=lambda x: len(x[1]))
    return candidates[0][0]


# --- Container scoping (Step 2) ---
CONTAINER_TAGS = {"article", "section", "table", "tbody", "tr"}

# Keywords that suggest a per-branch item container — we want to STOP here
ITEM_CLASS_KEYWORDS = ("item", "card", "branch", "location", "office", "widget")

# Keywords that suggest a broad shared wrapper — we want to SKIP past these
WRAPPER_CLASS_KEYWORDS = ("boxes", "list", "grid", "wrap", "container", "row", "section")


def _is_per_branch_container(tag) -> bool:
    """True if this tag looks like a single-branch card/item, not a shared wrapper."""
    if tag.name in CONTAINER_TAGS:
        return True
    cls = " ".join(tag.get("class") or []).lower()
    has_item = any(kw in cls for kw in ITEM_CLASS_KEYWORDS)
    has_wrapper = any(kw in cls for kw in WRAPPER_CLASS_KEYWORDS)
    # Prefer item-like classes; only treat as container if not purely a wrapper
    return has_item or (tag.name == "div" and has_wrapper and not has_item is False)


def _is_shared_wrapper(tag) -> bool:
    """True if this tag is a broad container likely holding multiple branches."""
    cls = " ".join(tag.get("class") or []).lower()
    return any(kw in cls for kw in WRAPPER_CLASS_KEYWORDS) and not any(kw in cls for kw in ITEM_CLASS_KEYWORDS)


def _has_content_beyond_heading(container, branch_node) -> bool:
    branch_text_len = len(_get_candidate_text(branch_node))
    container_text = (container.get_text(separator=" ", strip=True) or "")
    return len(container_text.strip()) > branch_text_len + 5


def _find_branch_container(branch_node):
    """
    Walk up from branch_node to find the smallest ancestor that is a
    per-branch container (item/card), stopping before any shared wrapper
    that holds multiple branches.
    """
    if not branch_node:
        return None

    best = None
    node = branch_node

    while node and node.parent and node.parent.name not in ("html", "body"):
        parent = node.parent
        cls = " ".join(parent.get("class") or []).lower()

        # Stop if we've reached a shared multi-branch wrapper
        if _is_shared_wrapper(parent) and not any(kw in cls for kw in ITEM_CLASS_KEYWORDS):
            break

        if _is_per_branch_container(parent) and _has_content_beyond_heading(parent, branch_node):
            best = parent  # Keep climbing to find the tightest fit

        node = parent

    return best or (branch_node.parent if branch_node.parent and branch_node.parent.name not in ("html", "body") else None)


def _next_element_sibling(tag):
    n = tag.next_sibling
    while n and not (hasattr(n, "name") and n.name):
        n = n.next_sibling if hasattr(n, "next_sibling") else None
    return n


def _find_table_branch_scope(branch_node):
    """
    Handle table-based layouts where the branch name is in a <th> inside one <tr>
    and the address is in a <td> inside the next <tr>.
    Returns a synthetic scope containing the next sibling <tr> content, or None.
    """
    # Walk up to the containing <tr>
    node = branch_node
    while node and node.name != "tr":
        node = node.parent
    if not node or node.name != "tr":
        return None

    # Gather this <tr> and the next 1-2 sibling <tr>s as the scope
    from bs4 import BeautifulSoup as BS
    scope_html = ""
    tr = node
    for _ in range(3):  # heading row + up to 2 data rows
        tr = _next_element_sibling(tr)
        if not tr or tr.name != "tr":
            break
        scope_html += str(tr)
    if not scope_html:
        return None
    return BS(f"<div>{scope_html}</div>", "html.parser").find("div")


# --- Address scoring (Step 3) ---
STREET_NUMBER = re.compile(r"\b\d{1,6}\s+\w+")
STREET_SUFFIXES = (
    "st", "street", "ave", "avenue", "rd", "road", "blvd", "boulevard",
    "ln", "lane", "dr", "drive", "hwy", "highway", "pkwy", "parkway",
    "ct", "court", "cir", "circle", "ter", "terrace", "way", "pl", "place",
)
CITY_STATE_ZIP = re.compile(r",\s*[A-Z]{2}\s*\d{5}(-\d{4})?", re.IGNORECASE)
PENALTY_LABELS = ("hours", "phone", "fax", "toll free")
MIN_ADDRESS_SCORE = 8


def _get_visible_text_with_br(el) -> str:
    if not el:
        return ""
    parts = []
    for child in el.descendants:
        if getattr(child, "name", None) == "br":
            parts.append("\n")
        elif hasattr(child, "string") and getattr(child, "name", None) is None:
            parts.append(str(child))
    text = "".join(parts) if parts else (el.get_text(separator=" ", strip=True) or "")
    return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()


def _score_address_candidate(text: str, has_address_label_nearby: bool = False) -> int:
    if not text or len(text) > 500:
        return -20
    score = 0
    if STREET_NUMBER.search(text):
        score += 6
    if any(" " + s + " " in " " + text.lower() + " " or text.lower().endswith(" " + s) for s in STREET_SUFFIXES):
        score += 6
    if CITY_STATE_ZIP.search(text):
        score += 6
    if has_address_label_nearby:
        score += 4
    if any(l in text.lower() for l in PENALTY_LABELS):
        if not CITY_STATE_ZIP.search(text) and not any(s in text.lower() for s in STREET_SUFFIXES):
            score -= 10
    if len(text) > 300 and text.lower().count("http") + text.lower().count("<") > 2:
        score -= 10
    return score


def _extract_address_from_full_text(text: str) -> Optional[str]:
    m = ADDRESS_PATTERN.search(text)
    if m:
        return m.group(0).strip()
    m = SIMPLE_ADDRESS.search(text)
    if m:
        return m.group(0).strip()
    return None


# --- Noise stripping & address extraction strategies (Step 3b) ---

def _strip_noise(text: str) -> str:
    """
    Remove noise lines (phone, hours, directions, etc.) from a text block.
    Works line-by-line so address lines survive intact.
    Also removes lines that look like bare phone numbers.
    """
    clean = []
    for line in re.split(r"[\n|]", text):
        line = line.strip().strip(",").strip()
        if not line:
            continue
        if _NOISE.search(line):
            continue
        if re.fullmatch(r"[\d\s\.\-\(\)\+]{7,}", line):
            continue
        clean.append(line)
    return ", ".join(clean)


def _text_from_br_block(tag) -> str:
    """
    Reconstruct text from a tag that uses <br> as line separator
    (common in Wix / plain-HTML sites). Returns joined non-empty lines.
    """
    parts = []
    for child in tag.children:
        if getattr(child, "name", None) == "br":
            parts.append("\n")
        elif hasattr(child, "get_text"):
            parts.append(child.get_text(strip=True))
        else:
            parts.append(str(child).strip())
    lines = [l.strip() for l in "".join(parts).split("\n") if l.strip()]
    return ", ".join(lines)


def _find_maps_link_address(scope) -> Optional[str]:
    """
    Strategy 1: address is the visible text of a maps hyperlink.
    Covers real maps URLs and local anchors with 'map' in href.
    Also checks for <strong> inside the link.
    """
    if not scope:
        return None
    for a in scope.find_all("a", href=True):
        href = a.get("href", "")
        if not _MAPS_HREF.search(href):
            continue
        strong = a.find("strong")
        text = (strong or a).get_text(separator=" ", strip=True)
        text = _strip_noise(text)
        if text and _score_address_candidate(text) >= MIN_ADDRESS_SCORE:
            return text
    return None


def _find_location_class_address(scope) -> Optional[str]:
    """
    Strategy 2: container has a class/id that explicitly names it as
    an address or location block.
    """
    if not scope:
        return None
    for tag in scope.find_all(True):
        cls = " ".join(tag.get("class") or [])
        tid = tag.get("id") or ""
        if not _ADDR_CLASS.search(cls) and not _ADDR_CLASS.search(tid):
            continue
        if tag.name == "a" and "tel:" in tag.get("href", ""):
            continue
        text = _strip_noise(tag.get_text(separator=" ", strip=True))
        if text and _score_address_candidate(text) >= MIN_ADDRESS_SCORE:
            return text
    return None


def _find_br_block_address(scope) -> Optional[str]:
    """
    Strategy 3: address split across <br>-separated lines inside a <p> or <div>
    (Wix pattern: "105 S. Wall St.<br>Floydada, TX 79235").
    Skips any block that contains a tel: link.
    """
    if not scope:
        return None
    for tag in scope.find_all(["p", "div", "td", "li"]):
        if tag.find("a", href=re.compile(r"^tel:", re.I)):
            continue
        if not tag.find("br"):
            continue
        text = _strip_noise(_text_from_br_block(tag))
        if text and _score_address_candidate(text) >= MIN_ADDRESS_SCORE:
            return text
    return None


def _find_address_in_scope(scope) -> Optional[str]:
    """
    Try five strategies in order of reliability:
      1. Maps hyperlink text (real URL or local #map anchor)
      2. Semantically-named location/address class
      3. <br>-separated lines in a <p>/<div> (Wix style)
      4. Best-scoring individual element (noise-stripped)
      5. Full container text regex fallback (noise-stripped)
    """
    if not scope:
        return None

    result = _find_maps_link_address(scope)
    if result:
        return result

    result = _find_location_class_address(scope)
    if result:
        return result

    result = _find_br_block_address(scope)
    if result:
        return result

    # Strategy 4: score individual candidate elements, noise-stripped
    best_score = -100
    best_text = None
    for tag in scope.find_all(["p", "div", "td", "strong", "span", "li"], recursive=True):
        if tag.find("a", href=re.compile(r"^tel:", re.I)):
            continue
        raw = _get_visible_text_with_br(tag).strip()
        if not raw or len(raw) < 10:
            continue
        text = _strip_noise(raw)
        if not text:
            continue
        prev = tag.find_previous_sibling()
        has_label = bool(prev and "address" in (prev.get_text() or "").lower())
        score = _score_address_candidate(text, has_label)
        if score >= MIN_ADDRESS_SCORE and score > best_score:
            best_score = score
            best_text = text

    if best_text:
        return best_text

    # Strategy 5: full container text, noise-stripped, regex-extracted
    full = _strip_noise(scope.get_text(separator="\n", strip=True))
    if len(full) >= 15:
        extracted = _extract_address_from_full_text(full)
        if extracted and _score_address_candidate(extracted) >= MIN_ADDRESS_SCORE:
            return extracted

    return None


# --- Clean and parse (Step 4) ---
CITY_STATE_ZIP_PARSE = re.compile(r"^(.*),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)\s*$", re.IGNORECASE | re.DOTALL)

STATES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    "MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC"
)


def _clean_address(raw: str) -> str:
    s = re.sub(r"\bAddress:\s*", "", raw, flags=re.IGNORECASE)
    s = re.sub(r"[.,;]+(\s*[.,;])+", ",", s)
    return s.strip()


def _parse_address(address_full: str) -> dict:
    street, city, state, zip_ = "", "", "", ""
    cleaned = _clean_address(address_full)
    m = CITY_STATE_ZIP_PARSE.match(cleaned.strip())
    if m:
        street = m.group(1).strip().rstrip(",")
        state = (m.group(2) or "").strip().upper()
        zip_ = (m.group(3) or "").strip()
        street_part = street
        if "," in street_part:
            parts = [p.strip() for p in street_part.split(",")]
            if len(parts) >= 2:
                city = parts[-1]
                street = ", ".join(parts[:-1])
            else:
                street = street_part
        else:
            street = street_part
    else:
        street = cleaned
    return {"street": street, "city": city, "state": state, "zip": zip_}


# --- Main API: match branch to address (Steps 1–5) ---
def match_branch_to_address(soup: BeautifulSoup, target_branch_name: str) -> dict:
    out = {
        "matched_branch_name": "",
        "address_full": "",
        "street": "",
        "city": "",
        "state": "",
        "zip": "",
    }
    branch_node = _find_branch_node(soup, target_branch_name)
    if not branch_node:
        return out

    matched_text = _get_candidate_text(branch_node)
    out["matched_branch_name"] = matched_text

    # For table layouts: address lives in the next sibling <tr>, not same container
    table_scope = _find_table_branch_scope(branch_node)
    if table_scope:
        address_full = _find_address_in_scope(table_scope)
    else:
        branch_container = _find_branch_container(branch_node)
        address_full = _find_address_in_scope(branch_container)
        if not address_full and branch_container:
            next_scope = _next_element_sibling(branch_container)
            if next_scope:
                address_full = _find_address_in_scope(next_scope)

    if not address_full:
        return out

    out["address_full"] = _clean_address(address_full)
    parsed = _parse_address(address_full)
    out["street"] = parsed["street"]
    out["city"] = parsed["city"]
    out["state"] = parsed["state"]
    out["zip"] = parsed["zip"]
    return out


# --- Scrape + geocode ---
def scrape_branches(url: str, branch_names: list) -> list:
    branch_names = [n.strip() for n in branch_names if n.strip()]
    if not branch_names:
        return []

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        return [{"error": str(e)}]

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    for branch in branch_names:
        row = {
            "branch_name": branch,
            "address": None,
            "latitude": None,
            "longitude": None,
        }
        match = match_branch_to_address(soup, branch)
        address_full = (match.get("address_full") or "").strip()
        if address_full:
            row["address"] = address_full
            lat, lon = geocode_address(address_full, match)
            row["latitude"] = lat
            row["longitude"] = lon
        results.append(row)

    return results


# --- Geocoding with multi-query variants + exponential backoff ---

_geocoder: Optional[Nominatim] = None
_last_geocode_time: float = 0.0

MIN_DELAY_SECONDS = 3.0
MAX_RETRIES = 6


def _get_geocoder() -> Nominatim:
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="credit-union-branch-scraper/1.0")
    return _geocoder


def _build_query_variants(address_full: str, parsed: dict) -> list[str]:
    """
    Build ordered list of query strings to try, from most to least specific.
    """
    queries = []
    street = parsed.get("street", "")
    city   = parsed.get("city", "")
    state  = parsed.get("state", "")
    zip_   = parsed.get("zip", "")

    # 1. Full structured query
    if street and city and state:
        queries.append(f"{street}, {city}, {state} {zip_}".strip())

    # 2. Suite/unit stripped
    clean_street = re.sub(r"\b(Suite|Ste|Unit|Apt|#)\s*\S+", "", street, flags=re.I).strip().rstrip(",").strip()
    if clean_street != street and city and state:
        queries.append(f"{clean_street}, {city}, {state} {zip_}".strip())

    # 3. Direction expanded (E → East, W → West, etc.)
    expanded = (street
        .replace(" E ", " East ").replace(" W ", " West ")
        .replace(" N ", " North ").replace(" S ", " South "))
    if expanded != street and city and state:
        queries.append(f"{expanded}, {city}, {state} {zip_}".strip())

    # 4. State Route normalized — fall back to city+state+zip
    if re.search(r"\b(state route|route|rte)\s*\d+", street, re.I) and city and state:
        queries.append(f"{city}, {state} {zip_}".strip())

    # 5. City + state + zip fallback
    if city and state and zip_:
        queries.append(f"{city}, {state} {zip_}")

    # 6. Raw address string as last resort
    if address_full.strip() not in queries:
        queries.append(address_full.strip())

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            deduped.append(q)
    return deduped


def _geocode_query(geocoder: Nominatim, query: str) -> Optional[tuple]:
    """
    Attempt to geocode a single query string with exponential backoff retries.
    Returns (lat, lon) on success, None on real miss, None on exhausted retries.
    """
    global _last_geocode_time

    for attempt in range(MAX_RETRIES):
        elapsed = time.time() - _last_geocode_time
        wait = MIN_DELAY_SECONDS * (2 ** attempt) if attempt > 0 else MIN_DELAY_SECONDS
        if elapsed < wait:
            time.sleep(wait - elapsed)

        try:
            _last_geocode_time = time.time()
            loc = geocoder.geocode(query, timeout=10)
            if loc:
                logger.info(f"  [GEO ✓] '{query}' → {loc.latitude}, {loc.longitude}")
                return (loc.latitude, loc.longitude)
            logger.info(f"  [GEO NONE MATCH] '{query}'")
            return None  # Real miss — no point retrying
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"  [GEO TIMEOUT/ERROR attempt {attempt + 1}/{MAX_RETRIES}] '{query}': {e}")
            if attempt == MAX_RETRIES - 1:
                logger.error(f"  [GEO FAIL FINAL] Exhausted retries for '{query}'")
                return None

    return None


def geocode_address(address_full: str, parsed_match: Optional[dict] = None) -> tuple:
    """
    Geocode an address string. Builds multiple query variants and tries each
    in order, with exponential backoff retries per query.
    """
    geocoder = _get_geocoder()
    parsed = parsed_match or _parse_address(address_full)
    queries = _build_query_variants(address_full, parsed)

    logger.info(f"[GEOCODE] '{address_full}' — trying {len(queries)} query variant(s)")

    for query in queries:
        result = _geocode_query(geocoder, query)
        if result is not None:
            return result

    logger.error(f"[GEO FAIL FINAL] No result for any variant of: '{address_full}'")
    return (None, None)


# --- Unit-test style examples ---
def _run_example_tests():
    from bs4 import BeautifulSoup as BS

    html1 = """
    <div class="branch-card">
      <h3>Main Branch</h3>
      <strong>123 Main Street</strong><br/>
      <strong>Springfield, IL 62701</strong>
    </div>
    """
    soup1 = BS(html1, "html.parser")
    r1 = match_branch_to_address(soup1, "Main Branch")
    assert r1["matched_branch_name"] == "Main Branch", r1
    assert "123" in (r1["address_full"] or ""), r1
    assert "62701" in (r1["address_full"] or ""), r1
    print("Example 1 (h3 + strong): OK")

    html2 = """
    <section class="contact-wrap">
      <span class="contact-hdr-back">Downtown Office</span>
      <p><strong>Address:</strong></p>
      <p>456 Oak Ave, Chicago, IL 60601</p>
    </section>
    """
    soup2 = BS(html2, "html.parser")
    r2 = match_branch_to_address(soup2, "Downtown Office")
    assert r2["matched_branch_name"] == "Downtown Office", r2
    assert "456" in (r2["address_full"] or "") and "60601" in (r2["address_full"] or ""), r2
    print("Example 2 (span.contact-hdr-back + Address:): OK")

    html3 = """
    <div class="elementor-widget">
      <h3 class="elementor-heading-title">Westside Branch</h3>
      <div class="elementor-text-editor">
        <a href="/locations"><strong>789 West Rd, Denver, CO 80202</strong></a>
      </div>
    </div>
    """
    soup3 = BS(html3, "html.parser")
    r3 = match_branch_to_address(soup3, "Westside Branch")
    assert r3["matched_branch_name"] == "Westside Branch", r3
    assert "789" in (r3["address_full"] or "") and "80202" in (r3["address_full"] or ""), r3
    print("Example 3 (elementor h3 + a/strong): OK")

    print("All example tests passed.")


if __name__ == "__main__":
    _run_example_tests()