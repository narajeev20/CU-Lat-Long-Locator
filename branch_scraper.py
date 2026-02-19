"""
Scrapes a credit union website for branch names and addresses,
then geocodes addresses to lat/long.

Branch-to-address matching is scoped: find the branch name node first,
then only search for address within the same local container (or next sibling).
"""
import re
import requests
from bs4 import BeautifulSoup
from typing import Optional

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

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
    # similarity: share of target tokens present in candidate
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
    # Prefer shortest text (most specific heading)
    candidates.sort(key=lambda x: len(x[1]))
    return candidates[0][0]


# --- Container scoping (Step 2) ---
CONTAINER_TAGS = {"article", "section", "table", "tbody", "tr"}
CONTAINER_CLASS_KEYWORDS = ("row", "container", "wrap", "card", "elementor", "widget")


def _is_container_like(tag) -> bool:
    if tag.name in CONTAINER_TAGS:
        return True
    cls = tag.get("class") or []
    joined = " ".join(cls).lower() if isinstance(cls, list) else str(cls).lower()
    return any(kw in joined for kw in CONTAINER_CLASS_KEYWORDS)


def _has_content_beyond_heading(container, branch_node) -> bool:
    """Container has more than just the branch heading."""
    branch_text_len = len(_get_candidate_text(branch_node))
    container_text = (container.get_text(separator=" ", strip=True) or "")
    return len(container_text.strip()) > branch_text_len + 5


def _find_branch_container(branch_node):
    """Walk up from branch_node to smallest ancestor that is a branch card/section."""
    if not branch_node:
        return None
    node = branch_node
    while node and node.parent and node.parent.name not in ("html", "body"):
        parent = node.parent
        if _is_container_like(parent) and _has_content_beyond_heading(parent, branch_node):
            return parent
        node = parent
    return branch_node.parent if branch_node.parent and branch_node.parent.name not in ("html", "body") else None


def _next_element_sibling(tag):
    """Next sibling that is a tag (element)."""
    n = tag.next_sibling
    while n and not (hasattr(n, "name") and n.name):
        n = n.next_sibling if hasattr(n, "next_sibling") else None
    return n


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
    """Visible text with <br> treated as newline, then collapse whitespace."""
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
    """Extract one address string from full text (for split-across-nodes)."""
    m = ADDRESS_PATTERN.search(text)
    if m:
        return m.group(0).strip()
    m = SIMPLE_ADDRESS.search(text)
    if m:
        return m.group(0).strip()
    return None


def _find_address_in_scope(scope) -> Optional[str]:
    """Collect candidate blocks from scope, score, return best address text or None."""
    if not scope:
        return None
    candidate_tags = scope.find_all(["p", "div", "td", "strong", "a", "span", "li"], recursive=True)
    best_score = -100
    best_text = None
    for tag in candidate_tags:
        text = _get_visible_text_with_br(tag).strip()
        if not text or len(text) < 10:
            continue
        prev = tag.find_previous_sibling()
        has_label = bool(prev and "address" in (prev.get_text() or "").lower())
        score = _score_address_candidate(text, has_label)
        if score >= MIN_ADDRESS_SCORE and score > best_score:
            best_score = score
            best_text = text

    # Fallback / prefer full scope when address is split across nodes (e.g. two <strong>)
    full = (scope.get_text(separator=" ", strip=True) or "").strip()
    if len(full) >= 15:
        extracted = _extract_address_from_full_text(full)
        if extracted and _score_address_candidate(extracted, False) >= MIN_ADDRESS_SCORE:
            # Prefer extracted if it has zip and best_text doesn't (split-address case)
            if re.search(r"\d{5}(?:-\d{4})?", extracted):
                if not best_text or not re.search(r"\d{5}(?:-\d{4})?", best_text):
                    best_text = extracted
            elif not best_text:
                best_text = extracted
    return best_text


# --- Clean and parse (Step 4) ---
CITY_STATE_ZIP_PARSE = re.compile(r"^(.*),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)\s*$", re.IGNORECASE | re.DOTALL)


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
        # city is last part of street before state (e.g. "City Name" in "123 Main St, City Name, CA 90210")
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
    """
    Given page HTML (as BeautifulSoup) and target_branch_name, return matched branch and address.
    Only searches for address within the local branch container (or its next sibling).
    """
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


# --- Scrape + geocode (existing app contract) ---
def scrape_branches(url: str, branch_names: list) -> list:
    """
    Scrape url for each branch name and associate address via scoped matching.
    Returns list of dicts: branch_name, address, latitude, longitude.
    """
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
            lat, lon = geocode_address(address_full)
            row["latitude"] = lat
            row["longitude"] = lon
        results.append(row)

    return results


_geocoder = None
_geocode_limited = None


def geocode_address(address: str) -> tuple:
    """Return (latitude, longitude) or (None, None)."""
    global _geocoder, _geocode_limited
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="credit-union-branch-scraper/1.0")
        _geocode_limited = RateLimiter(
            _geocoder.geocode,
            min_delay_seconds=2.0,
            max_retries=3,
            error_wait_seconds=10.0,
        )
    try:
        loc = _geocode_limited(address)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception:
        pass
    return (None, None)


# --- Unit-test style examples ---
def _run_example_tests():
    """Run example tests for branch-to-address matching."""
    from bs4 import BeautifulSoup as BS

    # Example 1: heading in <h3> with address in nearby <strong> lines
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

    # Example 2: heading in <span class="contact-hdr-back"> with address after <strong>Address:</strong>
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

    # Example 3: heading in <h3 class="elementor-heading-title"> with address inside <a> and <strong>
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
