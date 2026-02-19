# Credit Union Branch Scraper

A web app that scrapes a credit union (or any) website for branch locations and returns a table of **branch name**, **address**, **latitude**, and **longitude**.

## Setup

```bash
cd /Users/nityarajeev/python
python3 -m venv .venv   # if you don't already have one
source .venv/bin/activate   # or: .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Run

```bash
python app.py
```

Then open **http://127.0.0.1:5000** in your browser.

## How to use

1. **Website URL** – Enter the full URL of the page that lists branch locations (e.g. `https://example-cu.org/locations`).
2. **Branch names** – Enter each branch name separated by commas (e.g. `Main Branch, Downtown, Westside`). These are used to match each branch to its address on the page.
3. Click **Scrape branches**. The app will:
   - Fetch the page and parse it
   - Find each branch name and the address text near it (or elsewhere on the page)
   - Geocode addresses to latitude/longitude via Nominatim (OpenStreetMap)

Results appear in a table. If an address or coordinates can’t be found, that cell shows —.

## Notes

- **Geocoding** uses Nominatim and requires internet access. Rate limiting (1 request per second) is applied.
- **Scraping** works best on pages where branch names and addresses appear in the same section (e.g. same `<div>` or list item). Very dynamic or JavaScript-heavy pages may need a different approach.
- Respect the target site’s `robots.txt` and terms of use when scraping.

## Project layout

- `app.py` – Flask server and `/scrape` API
- `branch_scraper.py` – Scraping and geocoding logic
- `templates/index.html` – Single-page UI (form + results table)
- `requirements.txt` – Python dependencies
