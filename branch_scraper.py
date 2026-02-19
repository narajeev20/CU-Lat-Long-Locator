"""
Scrapes a credit union website for branch names and addresses,
then geocodes addresses to lat/long.
"""
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# US address pattern: number + street, optional suite; city; state (2 letters); zip (5 or 5+4)
ADDRESS_PATTERN = re.compile(
    r"\d+[\w\s\.\-]+(?:street|st|avenue|ave|blvd|boulevard|road|rd|drive|dr|lane|ln|way|court|ct|place|pl|suite|ste)[\w\s\.\-]*"
    r"[\s,]+[\w\s\.\-]+[\s,]+\b[A-Za-z]{2}\b[\s,]+\d{5}(?:-\d{4})?",
    re.IGNORECASE
)
# Simpler fallback: "City, ST 12345" or "City, ST"
SIMPLE_ADDRESS = re.compile(
    r"[\w\s\.\-]{3,50},?\s*[A-Za-z]{2}\s*,?\s*\d{5}(?:-\d{4})?",
    re.IGNORECASE
)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def _get_text_up_to_n_chars(el, max_chars=500):
    """Get text from element and its descendants, truncated."""
    if el is None:
        return ""
    text = el.get_text(separator=" ", strip=True)
    return text[:max_chars] if len(text) > max_chars else text


def _extract_address_from_text(text):
    """Try to find a single address string in text."""
    m = ADDRESS_PATTERN.search(text)
    if m:
        return m.group(0).strip()
    m = SIMPLE_ADDRESS.search(text)
    if m:
        return m.group(0).strip()
    return None


def _find_address_near_element(soup, start_el):
    """Look for address in element, siblings, and parent."""
    candidates = []
    if start_el:
        candidates.append(start_el)
        for s in start_el.find_next_siblings():
            candidates.append(s)
        for s in start_el.find_previous_siblings():
            candidates.append(s)
        p = start_el.parent
        if p and p.name not in ("html", "body"):
            candidates.append(p)
    for el in candidates:
        text = _get_text_up_to_n_chars(el)
        addr = _extract_address_from_text(text)
        if addr:
            return addr
    return None


def _find_all_addresses_in_page(soup):
    """Return list of address strings found anywhere in the page."""
    text = soup.get_text(separator=" ", strip=True)
    addrs = list(ADDRESS_PATTERN.findall(text))
    if not addrs:
        addrs = list(SIMPLE_ADDRESS.findall(text))
    # Also check structured data (schema.org, etc.)
    for tag in soup.find_all(True):
        if tag.get("itemprop") == "address" or (tag.get("class") and "address" in " ".join(tag.get("class", []))):
            t = _get_text_up_to_n_chars(tag, 300)
            a = _extract_address_from_text(t)
            if a and a not in addrs:
                addrs.append(a)
    return list(dict.fromkeys(addrs))


def scrape_branches(url, branch_names):
    """
    Scrape url for each branch name and try to associate an address.
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
    # Normalize whitespace in full page text for matching
    page_text = " ".join(soup.get_text().split())

    results = []
    all_addresses = _find_all_addresses_in_page(soup)

    for branch in branch_names:
        row = {
            "branch_name": branch,
            "address": None,
            "latitude": None,
            "longitude": None,
        }
        # Find element containing this branch name (exact or as word)
        pattern = re.compile(re.escape(branch), re.IGNORECASE)
        found_el = None
        for tag in soup.find_all(string=pattern):
            parent = tag.parent if hasattr(tag, "parent") else None
            if parent and parent.name:
                found_el = parent
                break
        if not found_el:
            for tag in soup.find_all(True):
                if pattern.search(_get_text_up_to_n_chars(tag, 200)):
                    found_el = tag
                    break

        addr = _find_address_near_element(soup, found_el) if found_el else None
        if not addr and all_addresses:
            # Fallback: use first unused address (weak association)
            for a in all_addresses:
                if a and not any(r.get("address") == a for r in results):
                    addr = a
                    break
        if addr:
            row["address"] = addr
            lat, lon = geocode_address(addr)
            row["latitude"] = lat
            row["longitude"] = lon
        results.append(row)

    return results


_geocoder = None
_geocode_limited = None


def geocode_address(address):
    """Return (latitude, longitude) or (None, None)."""
    global _geocoder, _geocode_limited
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="credit-union-branch-scraper/1.0")
        _geocode_limited = RateLimiter(_geocoder.geocode, min_delay_seconds=1.0)
    try:
        loc = _geocode_limited(address)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception:
        pass
    return (None, None)
