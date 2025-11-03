#!/usr/bin/env python3
"""
ETL: UGA Academic Calendars (Fall 2025 & Spring 2026)
- Extracts the first two "columns" from each term's schedule (Event, Date)
- Normalizes dates to MM-DD-YYYY (handles single dates and ranges)
- Outputs a CSV with columns: Term, event, date, source
"""

import re
import csv
from pathlib import Path
from typing import List, Tuple, Optional
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

# -----------------------
# Config
# -----------------------
SOURCE_URL = "https://reg.uga.edu/general-information/calendars/academic-calendars/"
TERMS = [
    ("Fall 2025", 2025),
    ("Spring 2026", 2026),  # <-- updated per your correction
]
OUTPUT_DIR = Path("data")
OUTPUT_CSV = OUTPUT_DIR / "events.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ETL-script/1.0; +https://example.org)"
}

MONTH_MAP = {
    "Jan": 1, "Jan.": 1, "January": 1,
    "Feb": 2, "Feb.": 2, "February": 2,
    "Mar": 3, "Mar.": 3, "March": 3,
    "Apr": 4, "Apr.": 4, "April": 4,
    "May": 5,
    "Jun": 6, "Jun.": 6, "June": 6,
    "Jul": 7, "Jul.": 7, "July": 7,
    "Aug": 8, "Aug.": 8, "August": 8,
    "Sep": 9, "Sep.": 9, "Sept.": 9, "September": 9,
    "Oct": 10, "Oct.": 10, "October": 10,
    "Nov": 11, "Nov.": 11, "November": 11,
    "Dec": 12, "Dec.": 12, "December": 12,
}

MONTH_PATTERN = r"(?:Jan\.?|January|Feb\.?|February|Mar\.?|March|Apr\.?|April|May|Jun\.?|June|Jul\.?|July|Aug\.?|August|Sep\.?|Sept\.?|September|Oct\.?|October|Nov\.?|November|Dec\.?|December)"
MD_PAIR = re.compile(rf"({MONTH_PATTERN})\s+(\d{{1,2}})")
DOW_WORDS = {"monday","tuesday","wednesday","thursday","friday","saturday","sunday"}

def fetch_html(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def heading_level(tag_name: str) -> int:
    if tag_name and tag_name.lower().startswith("h") and tag_name[1:].isdigit():
        return int(tag_name[1:])
    return 99

def iter_term_block(root: BeautifulSoup, term_title: str) -> List[str]:
    heading = None
    for h in root.find_all(re.compile(r"^h[1-6]$")):
        if h.get_text(strip=True) == term_title:
            heading = h
            break
    if heading is None:
        return []

    lvl = heading_level(heading.name)
    lines: List[str] = []

    for sib in heading.next_siblings:
        if isinstance(sib, NavigableString):
            continue
        if isinstance(sib, Tag):
            if sib.name and re.fullmatch(r"h[1-6]", sib.name):
                if heading_level(sib.name) <= lvl:
                    break
            if sib.name in ("h5", "h6", "strong"):
                continue
            if sib.name in ("p", "div", "section"):
                lines.extend(_extract_lines_from_block(sib))
            elif sib.name in ("ul", "ol"):
                for li in sib.find_all("li"):
                    lines.extend(_extract_lines_from_block(li))
            elif sib.name == "table":
                for tr in sib.find_all("tr"):
                    tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
                    if len(tds) >= 2:
                        lines.append(f"{tds[0]}  {tds[1]}")

    clean = []
    seen = set()
    for line in lines:
        s = " ".join(line.split())
        if not s:
            continue
        if s.lower().startswith("based on") or s.lower().startswith("approved"):
            continue
        if s not in seen:
            seen.add(s)
            clean.append(s)
    return clean

def _extract_lines_from_block(tag: Tag) -> List[str]:
    if tag.find("br"):
        parts = []
        curr = []
        for elem in tag.children:
            if isinstance(elem, NavigableString):
                curr.append(str(elem))
            elif isinstance(elem, Tag) and elem.name == "br":
                part = " ".join("".join(curr).split())
                if part:
                    parts.append(part)
                curr = []
            elif isinstance(elem, Tag):
                curr.append(elem.get_text(" ", strip=True))
        last = " ".join("".join(curr).split())
        if last:
            parts.append(last)
        return parts
    else:
        txt = tag.get_text(" ", strip=True)
        return [txt] if txt else []

def split_event_and_date(line: str) -> Optional[Tuple[str, str]]:
    m = MD_PAIR.search(line)
    if not m:
        return None
    start = m.start()
    event = line[:start].strip().rstrip(":-\u2013\u2014")
    date_chunk = line[start:].strip()
    return event, date_chunk

def normalize_date_chunk(date_chunk: str, default_year: int) -> str:
    s = (date_chunk
         .replace("\u2013", "-")
         .replace("\u2014", "-")
         .replace("–", "-")
         .replace("—", "-"))
    low = s.lower()
    for w in DOW_WORDS:
        low = re.sub(rf"\b{w}\b", "", low)
    s = " ".join(low.split())

    pairs = MD_PAIR.findall(s)
    if len(pairs) >= 2:
        (m1, d1), (m2, d2) = pairs[0], pairs[1]
        start = _fmt(int(MONTH_MAP[m1]), int(d1), default_year)
        end   = _fmt(int(MONTH_MAP[m2]), int(d2), default_year)
        return f"{start} to {end}"
    elif len(pairs) == 1:
        (m1, d1) = pairs[0]
        after = s[MD_PAIR.search(s).end():]
        m2day = re.search(r"-\s*(\d{1,2})\b", after)
        if m2day:
            start = _fmt(int(MONTH_MAP[m1]), int(d1), default_year)
            end   = _fmt(int(MONTH_MAP[m1]), int(m2day.group(1)), default_year)
            return f"{start} to {end}"
        else:
            return _fmt(int(MONTH_MAP[m1]), int(d1), default_year)

    return " ".join(date_chunk.split())

def _fmt(mm: int, dd: int, yyyy: int) -> str:
    return f"{mm:02d}-{dd:02d}-{yyyy}"

def extract_term_rows(soup: BeautifulSoup, term_title: str, year: int) -> List[Tuple[str, str]]:
    lines = iter_term_block(soup, term_title)
    rows: List[Tuple[str, str]] = []
    for raw in lines:
        maybe = split_event_and_date(raw)
        if not maybe:
            continue
        event, date_chunk = maybe
        if not event:
            continue
        norm_date = normalize_date_chunk(date_chunk, default_year=year)
        rows.append((event, norm_date))
    return rows

def main():
    print(f"[ETL] Fetching: {SOURCE_URL}")
    soup = fetch_html(SOURCE_URL)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Term", "event", "date", "source"])
        for term_title, year in TERMS:
            rows = extract_term_rows(soup, term_title, year)
            for event, date_str in rows:
                writer.writerow([term_title, event, date_str, SOURCE_URL])

    print(f"[ETL] Wrote {OUTPUT_CSV.resolve()}")

if __name__ == "__main__":
    main()
