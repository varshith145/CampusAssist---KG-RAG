#!/usr/bin/env python3
"""
All-in-one ETL + Import to Neo4j

What it does:
1) Scrape UGA Academic Calendars (Fall 2025, Spring 2026)
2) Write data -> CSV (events.csv)
3) Import into Neo4j (choose one)
   A) Direct insert via Neo4j driver (recommended)
   B) LOAD CSV from Neo4j Desktop 'import' folder (requires you pass that path)

USAGE (Recommended - Option A):
  python etl_and_import.py --method driver \
      --bolt bolt://localhost:7687 \
      --user neo4j --password your_password \
      --db neo4j

USAGE (Option B: LOAD CSV):
  python etl_and_import.py --method loadcsv \
      --import-dir "C:\\Users\\Owner\\.Neo4j\\relate-data\\dbmss\\<dbms-id>\\import" \
      --bolt bolt://localhost:7687 \
      --user neo4j --password your_password \
      --db neo4j

Prereqs:
  pip install requests beautifulsoup4 neo4j
"""

import argparse
import csv
import os
import re
from pathlib import Path
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from neo4j import GraphDatabase

SOURCE_URL = "https://reg.uga.edu/general-information/calendars/academic-calendars/"
TERMS = [
    ("Fall 2025", 2025),
    ("Spring 2026", 2026),
]

# ---------- scraping helpers (same as before, trimmed) ----------

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
    hdrs = {"User-Agent": "Mozilla/5.0 (ETL/1.0)"}
    r = requests.get(url, headers=hdrs, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def heading_level(tag_name: str) -> int:
    if tag_name and tag_name.lower().startswith("h") and tag_name[1:].isdigit():
        return int(tag_name[1:])
    return 99

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
            if sib.name in ("h5","h6","strong"):
                continue
            if sib.name in ("p","div","section"):
                lines.extend(_extract_lines_from_block(sib))
            elif sib.name in ("ul","ol"):
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

def split_event_and_date(line: str) -> Optional[Tuple[str, str]]:
    m = MD_PAIR.search(line)
    if not m:
        return None
    start = m.start()
    event = line[:start].strip().rstrip(":-\u2013\u2014")
    date_chunk = line[start:].strip()
    return event, date_chunk

def _fmt(mm: int, dd: int, yyyy: int) -> str:
    return f"{mm:02d}-{dd:02d}-{yyyy}"

def normalize_date_chunk(date_chunk: str, default_year: int) -> str:
    s = (date_chunk
         .replace("\u2013", "-").replace("\u2014", "-")
         .replace("–", "-").replace("—", "-"))
    low = s.lower()
    for w in DOW_WORDS:
        low = re.sub(rf"\b{w}\b", "", low)
    s = " ".join(low.split())
    pairs = MD_PAIR.findall(s)
    if len(pairs) >= 2:
        (m1,d1),(m2,d2) = pairs[0], pairs[1]
        return f"{_fmt(MONTH_MAP[m1], int(d1), default_year)} to {_fmt(MONTH_MAP[m2], int(d2), default_year)}"
    elif len(pairs) == 1:
        (m1,d1) = pairs[0]
        after = s[MD_PAIR.search(s).end():]
        m2day = re.search(r"-\s*(\d{1,2})\b", after)
        if m2day:
            return f"{_fmt(MONTH_MAP[m1], int(d1), default_year)} to {_fmt(MONTH_MAP[m1], int(m2day.group(1)), default_year)}"
        else:
            return _fmt(MONTH_MAP[m1], int(d1), default_year)
    return " ".join(date_chunk.split())

def extract_term_rows(soup: BeautifulSoup, term_title: str, year: int) -> List[Tuple[str,str]]:
    rows = []
    for raw in iter_term_block(soup, term_title):
        maybe = split_event_and_date(raw)
        if not maybe:
            continue
        event, date_chunk = maybe
        if not event:
            continue
        rows.append((event, normalize_date_chunk(date_chunk, year)))
    return rows

# ---------- import helpers ----------

def ensure_schema(session):
    # Optional: safe to run every time
    session.run("""
    CREATE CONSTRAINT event_node_key IF NOT EXISTS
    FOR (e:Event)
    REQUIRE (e.term, e.event, e.date) IS NODE KEY;
    """)
    session.run("CREATE INDEX event_term IF NOT EXISTS FOR (e:Event) ON (e.term);")
    session.run("CREATE INDEX event_name IF NOT EXISTS FOR (e:Event) ON (e.event);")

def import_via_driver(uri: str, user: str, password: str, db: str, rows: List[Tuple[str,str,str,str]]):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver, driver.session(database=db) as session:
        ensure_schema(session)
        # Parameterized UNWIND = fast bulk upsert
        session.run("""
        UNWIND $rows AS r
        MERGE (e:Event { term: r.term, event: r.event, date: r.date })
        SET e.source = r.source
        """, rows=[{"term": t, "event": ev, "date": dt, "source": src} for (t, ev, dt, src) in rows])

def write_csv(csv_path: Path, rows: List[Tuple[str,str,str,str]]):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Term", "event", "date", "source"])
        w.writerows(rows)

def import_via_loadcsv(uri: str, user: str, password: str, db: str, import_dir: Path, csv_name: str):
    """
    Writes the CSV into import_dir (creates folder if missing) then runs LOAD CSV.
    """
    import_dir.mkdir(parents=True, exist_ok=True)
    # Neo4j expects the file to be named exactly and referenced as file:///NAME.csv
    csv_path = import_dir / csv_name

    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver, driver.session(database=db) as session:
        ensure_schema(session)
        # LOAD CSV (Browser security reads only from import/)
        session.run("""
        LOAD CSV WITH HEADERS FROM $url AS row
        MERGE (e:Event { term: row.Term, event: row.event, date: row.date })
        SET e.source = row.source
        """, url=f"file:///{csv_name}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--method", choices=["driver","loadcsv"], required=True,
                        help="driver = direct insert with Neo4j driver (recommended); loadcsv = write into import/ then LOAD CSV")
    parser.add_argument("--bolt", required=True, help="bolt://host:7687")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--db", default="neo4j")
    parser.add_argument("--csv-out", default="data/events.csv", help="Where to save the CSV we generate")
    parser.add_argument("--import-dir", default="", help="(Only for --method loadcsv) Path to Neo4j Desktop import folder")
    parser.add_argument("--csv-name", default="events.csv", help="(Only for --method loadcsv) Name of CSV inside import/")
    args = parser.parse_args()

    # 1) Scrape + assemble rows
    soup = fetch_html(SOURCE_URL)
    rows = []
    for term, year in TERMS:
        for event, date in extract_term_rows(soup, term, year):
            rows.append((term, event, date, SOURCE_URL))

    # 2) Write CSV (for your artifacts)
    csv_out_path = Path(args.csv_out)
    write_csv(csv_out_path, rows)
    print(f"[OK] Wrote CSV -> {csv_out_path.resolve()}  (rows: {len(rows)})")

    # 3) Import
    if args.method == "driver":
        import_via_driver(args.bolt, args.user, args.password, args.db, rows)
        print("[OK] Imported via Neo4j driver (no import/ folder needed).")
    else:
        if not args.import_dir:
            raise SystemExit("--import-dir is required for --method loadcsv")
        # Copy CSV to import dir
        # (we already wrote it to csv_out_path; now copy its contents)
        import_dir = Path(args.import_dir)
        import_dir.mkdir(parents=True, exist_ok=True)
        dest = import_dir / args.csv_name
        if csv_out_path.resolve() != dest.resolve():
            dest.write_bytes(csv_out_path.read_bytes())
        import_via_loadcsv(args.bolt, args.user, args.password, args.db, import_dir, args.csv_name)
        print(f"[OK] Imported via LOAD CSV from {dest}")
    print("[DONE] ETL + import complete.")

if __name__ == "__main__":
    main()
