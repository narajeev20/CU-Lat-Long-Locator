"""
Scrapes a credit union website for branch names and addresses,
then geocodes addresses to lat/long.
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

ADDRESS_PATTERN = re.compile(
    r"\d{1,6}[\w\s\.\-]*(?:street|st|avenue|ave|blvd|boulevard|road|rd|drive|dr|lane|ln|way|court|ct|place|pl|suite|ste)[\w\s\.\-]*"
    r"[\s,]+[\w\s\.\-]+[\s,]+\b[A-Za-z]{2}\b[\s,]+\d{5}(?:-\d{4})?",
    re.IGNORECASE,
)
SIMPLE_ADDRESS = re.compile(
    r"[\w\s\.\-]{3,80},?\s*[A-Za-z]{2}\s*,?\s*\d{5}(?:-\d{4})?",
    re.IGNORECASE,
)

_MAPS_HREF = re.compile(
    r"(maps\.app\.goo\.gl|maps\.google\.|goo\.gl/maps|apple\.com/maps"
    r"|bing\.com/maps|mapquest\.com|#.*map)",
    re.IGNORECASE,
)

_ADDR_CLASS = re.compile(
    r"(address|location|branch.?info|contact.?info|company_information__location)",
    re.IGNORECASE,
)

_NOISE = re.compile(
    r"(phone|fax|tel:|toll.?free|hours?|open|closed|"
    r"mon|tue|wed|thu|fri|sat|sun|"
    r"drive.?thru|lobby|directions|watch|webcam|call\s|"
    r"get\s+directions|view\s+map|click\s+here)",
    re.IGNORECASE,
)

# ── Normalize ──────────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    if not text:
        return ""
    s = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
    return " ".join(s.split()).strip()


def _tokens(s: str):
    return set(normalize(s).split()) if normalize(s) else set()


BRANCH_HEADING_TAGS = {"h1", "h2", "h3", "h4", "th", "strong", "span", "a", "div", "p"}


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


# ── Container scoping ──────────────────────────────────────────────────────

CONTAINER_TAGS = {"article", "section", "table", "tbody", "tr"}
ITEM_CLASS_KEYWORDS = ("item", "card", "branch", "location", "office", "widget")
WRAPPER_CLASS_KEYWORDS = ("boxes", "list", "grid", "wrap", "container", "row", "section")


def _is_per_branch_container(tag) -> bool:
    if tag.name in CONTAINER_TAGS:
        return True
    cls = " ".join(tag.get("class") or []).lower()
    has_item = any(kw in cls for kw in ITEM_CLASS_KEYWORDS)
    has_wrapper = any(kw in cls for kw in WRAPPER_CLASS_KEYWORDS)
    return has_item or (tag.name == "div" and has_wrapper and not has_item is False)


def _is_shared_wrapper(tag) -> bool:
    cls = " ".join(tag.get("class") or []).lower()
    return any(kw in cls for kw in WRAPPER_CLASS_KEYWORDS) and not any(kw in cls for kw in ITEM_CLASS_KEYWORDS)


def _has_content_beyond_heading(container, branch_node) -> bool:
    branch_text_len = len(_get_candidate_text(branch_node))
    container_text = (container.get_text(separator=" ", strip=True) or "")
    return len(container_text.strip()) > branch_text_len + 5


def _find_branch_container(branch_node):
    if not branch_node:
        return None
    best = None
    node = branch_node
    while node and node.parent and node.parent.name not in ("html", "body"):
        parent = node.parent
        cls = " ".join(parent.get("class") or []).lower()
        if _is_shared_wrapper(parent) and not any(kw in cls for kw in ITEM_CLASS_KEYWORDS):
            break
        if _is_per_branch_container(parent) and _has_content_beyond_heading(parent, branch_node):
            best = parent
        node = parent
    return best or (branch_node.parent if branch_node.parent and branch_node.parent.name not in ("html", "body") else None)


def _next_element_sibling(tag):
    n = tag.next_sibling
    while n and not (hasattr(n, "name") and n.name):
        n = n.next_sibling if hasattr(n, "next_sibling") else None
    return n


# ── Address scoring ────────────────────────────────────────────────────────

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


# ── Noise stripping & extraction strategies ────────────────────────────────

def _strip_noise(text: str) -> str:
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

    full = _strip_noise(scope.get_text(separator="\n", strip=True))
    if len(full) >= 15:
        extracted = _extract_address_from_full_text(full)
        if extracted and _score_address_candidate(extracted) >= MIN_ADDRESS_SCORE:
            return extracted

    return None


# ── Table address map (pre-scan all tables) ────────────────────────────────

def _build_table_address_map(soup: BeautifulSoup) -> dict:
    """
    Pre-scan all tables and build normalized branch name -> address text.
    Handles: <tr><th>Branch Name</th></tr> followed by <tr><td>address</td></tr>
    """
    table_map = {}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for i, row in enumerate(rows):
            ths = row.find_all("th")
            if not ths:
                continue
            branch_text = ths[0].get_text(strip=True)
            if not branch_text or len(branch_text) > 100:
                continue
            # Search following rows for address
            for j in range(i + 1, min(i + 4, len(rows))):
                next_row = rows[j]
                if next_row.find("th"):
                    break  # hit next branch heading
                tds = next_row.find_all("td", recursive=False)
                if not tds:
                    continue
                first_td = tds[0]
                if first_td.find("a", href=re.compile(r"^tel:", re.I)):
                    continue
                addr_text = _strip_noise(_text_from_br_block(first_td))
                if addr_text and _score_address_candidate(addr_text) >= MIN_ADDRESS_SCORE:
                    table_map[normalize(branch_text)] = addr_text
                    break
    return table_map


# ── Clean and parse ────────────────────────────────────────────────────────

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


# ── Main match function ────────────────────────────────────────────────────

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

    out["matched_branch_name"] = _get_candidate_text(branch_node)

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


# ── Scrape + geocode ───────────────────────────────────────────────────────

def scrape_branches(url: str, branch_names: list) -> list:
    branch_names = [n.strip() for n in branch_names if n.strip()]
    if not branch_names:
        return []

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        return [{"error": str(e)}]

    # Parse full HTML but deduplicate by only using content from the first
    # occurrence of the main content container to avoid duplicate matches.
    raw_html = resp.text
    soup_full = BeautifulSoup(raw_html, "html.parser")

    # Try to find the first main content div to scope everything
    main_content = (
        soup_full.find("div", id="interiorContent") or
        soup_full.find("main") or
        soup_full.find("article") or
        soup_full.find("div", class_=re.compile(r"content|main|page", re.I))
    )
    soup = BeautifulSoup(str(main_content), "html.parser") if main_content else soup_full

    # Pre-scan all tables for branch->address mappings
    table_address_map = _build_table_address_map(soup)

    results = []
    used_addresses: set = set()  # track claimed addresses to prevent reuse

    for branch in branch_names:
        row = {
            "branch_name": branch,
            "address": None,
            "latitude": None,
            "longitude": None,
        }

        # 1. Try table map first (fast, reliable for th/td layouts)
        address_full = table_address_map.get(normalize(branch), "")
        match = {}

        # 2. Fall back to DOM-based matching
        if not address_full:
            match = match_branch_to_address(soup, branch)
            address_full = (match.get("address_full") or "").strip()

        # 3. Reject if this address was already claimed by a previous branch
        addr_key = normalize(address_full)
        if address_full and addr_key in used_addresses:
            logger.warning(f"[DEDUP] '{branch}' got duplicate address '{address_full}' — skipping")
            address_full = ""

        if address_full:
            used_addresses.add(addr_key)
            row["address"] = address_full
            lat, lon = geocode_address(address_full, match or None)
            row["latitude"] = lat
            row["longitude"] = lon

        results.append(row)

    return results


# ── Geocoding ──────────────────────────────────────────────────────────────

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
    queries = []
    street = parsed.get("street", "")
    city   = parsed.get("city", "")
    state  = parsed.get("state", "")
    zip_   = parsed.get("zip", "")

    if street and city and state:
        queries.append(f"{street}, {city}, {state} {zip_}".strip())

    clean_street = re.sub(r"\b(Suite|Ste|Unit|Apt|#)\s*\S+", "", street, flags=re.I).strip().rstrip(",").strip()
    if clean_street != street and city and state:
        queries.append(f"{clean_street}, {city}, {state} {zip_}".strip())

    expanded = (street
        .replace(" E ", " East ").replace(" W ", " West ")
        .replace(" N ", " North ").replace(" S ", " South "))
    if expanded != street and city and state:
        queries.append(f"{expanded}, {city}, {state} {zip_}".strip())

    if re.search(r"\b(state route|route|rte)\s*\d+", street, re.I) and city and state:
        queries.append(f"{city}, {state} {zip_}".strip())

    if city and state and zip_:
        queries.append(f"{city}, {state} {zip_}")

    if address_full.strip() not in queries:
        queries.append(address_full.strip())

    seen = set()
    deduped = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            deduped.append(q)
    return deduped


def _geocode_query(geocoder: Nominatim, query: str) -> Optional[tuple]:
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
            return None
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"  [GEO TIMEOUT/ERROR attempt {attempt + 1}/{MAX_RETRIES}] '{query}': {e}")
            if attempt == MAX_RETRIES - 1:
                logger.error(f"  [GEO FAIL FINAL] Exhausted retries for '{query}'")
                return None

    return None


def geocode_address(address_full: str, parsed_match: Optional[dict] = None) -> tuple:
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


# ── Tests ──────────────────────────────────────────────────────────────────

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
    assert "123" in (r1["address_full"] or ""), r1
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
    assert "456" in (r2["address_full"] or ""), r2
    print("Example 2 (span + Address:): OK")

    html3 = """
    <table>
      <tbody>
        <tr><th colspan="2"><strong>Cody</strong></th></tr>
        <tr>
          <td>1702 17th Street<br>Cody, Wyoming 82414</td>
          <td>Phone: (307) 587-4915</td>
        </tr>
        <tr><th colspan="2"><strong>Powell</strong></th></tr>
        <tr>
          <td>374 N. Clark<br>Powell, Wyoming 82435</td>
          <td>Phone: (307) 754-7191</td>
        </tr>
      </tbody>
    </table>
    """
    soup3 = BS(html3, "html.parser")
    tmap = _build_table_address_map(soup3)
    assert "cody" in tmap, tmap
    assert "powell" in tmap, tmap
    assert "82414" in tmap["cody"], tmap
    assert "82435" in tmap["powell"], tmap
    print("Example 3 (table th/td map): OK")

    print("All tests passed.")


if __name__ == "__main__":
    _run_example_tests()