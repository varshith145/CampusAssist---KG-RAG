from __future__ import annotations

import os
import sys
import re
import json
from typing import List, Dict, Any, Optional, Tuple

import requests
from neo4j import GraphDatabase

try:
    from tabulate import tabulate
    HAVE_TABULATE = True
except ImportError:
    HAVE_TABULATE = False


# ---------- CONFIG: EDIT THESE FOR YOUR SETUP ----------
NEO4J_URI = "bolt://localhost:7687"   # or "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Chintu@2025"

OLLAMA_HOST = "http://localhost:11434"
OLLAMA_MODEL = "llama3.2:3b"


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}


# ---------- NEO4J HELPER ----------
def run_cypher(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as s:
            return s.run(query, params or {}).data()
    finally:
        driver.close()


# ---------- OLLAMA HELPER ----------
def call_ollama(prompt: str, temperature: float = 0.2) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "options": {"temperature": temperature},
    }
    with requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json=payload,
        stream=True,
        timeout=600,
    ) as r:
        r.raise_for_status()
        chunks: List[str] = []
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                data = json.loads(line)
                if "response" in data:
                    chunks.append(data["response"])
                if data.get("done"):
                    break
            except json.JSONDecodeError:
                chunks.append(line)
        return "".join(chunks).strip()


# ---------- QUESTION PARSING ----------
def extract_term(question: str) -> Optional[str]:
    """
    Match 'Fall 2025', 'Spring 2026', etc.
    """
    m = re.search(r"\b(Fall|Spring|Summer)\s+(\d{4})\b", question, re.IGNORECASE)
    if not m:
        return None
    return f"{m.group(1).title()} {m.group(2)}"


def extract_weekday(question: str) -> Optional[str]:
    ql = question.lower()
    for wd in WEEKDAYS:
        if wd.lower() in ql or (wd.lower() + "s") in ql:
            return wd
    return None


def extract_month(question: str) -> Optional[Tuple[int, int]]:
    """
    Returns (year, month) if found, otherwise None.
    Needs a year in the question like 2025.
    """
    ql = question.lower()
    found_month = None
    for name, num in MONTHS.items():
        if name in ql:
            found_month = num
            break
    if not found_month:
        return None

    m_year = re.search(r"\b(20\d{2})\b", ql)
    if not m_year:
        return None
    year = int(m_year.group(1))
    return year, found_month


def detect_anchor(question: str) -> Optional[str]:
    """
    Detect anchor events like Classes Begin / Classes End
    """
    ql = question.lower()
    if "classes end" in ql or "class end" in ql or "end of classes" in ql:
        return "Classes End"
    if "classes begin" in ql or "classes start" in ql or "start of classes" in ql:
        return "Classes Begin"
    return None


def classify_intent(question: str) -> str:
    """
    Determine which kind of query this is, based on simple rules.

    Returns one of:
    - 'overlaps'
    - 'same_day'
    - 'after_anchor'
    - 'before_anchor'
    - 'weekday'
    - 'month'
    - 'classes_start'
    - 'all_events'
    """
    ql = question.lower()

    if "overlap" in ql or "overlapping" in ql:
        return "overlaps"
    if "same day" in ql or "same-day" in ql:
        return "same_day"
    if "after" in ql and detect_anchor(question):
        return "after_anchor"
    if "before" in ql and detect_anchor(question):
        return "before_anchor"
    if "start" in ql and "class" in ql:
        return "classes_start"
    if extract_weekday(question):
        return "weekday"
    if extract_month(question):
        return "month"
    return "all_events"


# ---------- QUERY BUILDERS (Cypher templates) ----------
def q_classes_start(term: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE toLower(e.name) = 'classes begin'
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.start_weekday        AS weekday,
      e.source               AS source
    """
    return q, {"term": term}


def q_after_anchor(term: str, anchor: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(anchor:Event)
    WHERE toLower(anchor.name) = toLower($anchor)
    WITH anchor.start_date AS anchor_date

    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date > anchor_date
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.start_weekday        AS weekday,
      e.source               AS source
    ORDER BY e.start_date
    """
    return q, {"term": term, "anchor": anchor}


def q_before_anchor(term: str, anchor: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(anchor:Event)
    WHERE toLower(anchor.name) = toLower($anchor)
    WITH anchor.start_date AS anchor_date

    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date < anchor_date
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.start_weekday        AS weekday,
      e.source               AS source
    ORDER BY e.start_date
    """
    return q, {"term": term, "anchor": anchor}


def q_weekday(term: str, weekday: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_weekday = $weekday
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.source               AS source
    ORDER BY e.start_date
    """
    return q, {"term": term, "weekday": weekday}


def q_month(term: str, year: int, month: int) -> Tuple[str, Dict[str, Any]]:
    """
    Events in a given year-month window: [start, next_month_start)
    """
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1

    start = f"{year:04d}-{month:02d}-01"
    end = f"{next_year:04d}-{next_month:02d}-01"

    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date >= date($start)
      AND e.start_date <  date($end)
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.start_weekday        AS weekday,
      e.source               AS source
    ORDER BY e.start_date
    """
    return q, {"term": term, "start": start, "end": end}


def q_overlaps(term: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),
          (:Term {name:$term})-[:HAS_EVENT]->(b:Event)
    WHERE a.start_date <= b.end_date
      AND b.start_date <= a.end_date
      AND id(a) < id(b)
    RETURN
      a.name                 AS event1,
      toString(a.start_date) AS a_start,
      toString(a.end_date)   AS a_end,
      b.name                 AS event2,
      toString(b.start_date) AS b_start,
      toString(b.end_date)   AS b_end
    ORDER BY a_start, b_start
    """
    return q, {"term": term}


def q_same_day(term: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),
          (:Term {name:$term})-[:HAS_EVENT]->(b:Event)
    WHERE a.start_date = b.start_date
      AND id(a) < id(b)
    RETURN
      a.name                 AS event1,
      b.name                 AS event2,
      toString(a.start_date) AS date
    ORDER BY date
    """
    return q, {"term": term}


def q_all_events(term: str) -> Tuple[str, Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    RETURN
      e.name                 AS name,
      toString(e.start_date) AS start_date,
      toString(e.end_date)   AS end_date,
      e.start_weekday        AS weekday,
      e.source               AS source
    ORDER BY e.start_date
    """
    return q, {"term": term}


def build_query_from_question(question: str) -> Tuple[str, Dict[str, Any], str, str, str]:
    """
    Decide which Cypher template to use, based on the question text.

    Returns (cypher, params, mode_description, intent, term).
    """
    term = extract_term(question) or "Fall 2025"
    intent = classify_intent(question)
    anchor = detect_anchor(question)
    wd = extract_weekday(question)
    month_info = extract_month(question)

    if intent == "classes_start":
        cy, p = q_classes_start(term)
        return cy, p, f"Classes start date for {term}", intent, term

    if intent == "after_anchor" and anchor:
        cy, p = q_after_anchor(term, anchor)
        return cy, p, f"Events in {term} after '{anchor}'", intent, term

    if intent == "before_anchor" and anchor:
        cy, p = q_before_anchor(term, anchor)
        return cy, p, f"Events in {term} before '{anchor}'", intent, term

    if intent == "weekday" and wd:
        cy, p = q_weekday(term, wd)
        return cy, p, f"{wd} events in {term}", intent, term

    if intent == "month" and month_info:
        year, month = month_info
        cy, p = q_month(term, year, month)
        return cy, p, f"Events in {term} during {year}-{month:02d}", intent, term

    if intent == "overlaps":
        cy, p = q_overlaps(term)
        return cy, p, f"Overlapping events in {term}", intent, term

    if intent == "same_day":
        cy, p = q_same_day(term)
        return cy, p, f"Same-day event pairs in {term}", intent, term

    # Default: list all events
    cy, p = q_all_events(term)
    return cy, p, f"All events in {term}", "all_events", term


# ---------- BUILD FACTUAL SUMMARY FROM ROWS ----------
def build_factual_summary(
    question: str,
    intent: str,
    term: str,
    rows: List[Dict[str, Any]],
) -> str:
    """
    Build a plain factual summary string FROM THE ROWS ONLY.
    The LLM will only rewrite this; it should not change any facts.
    """
    n = len(rows)
    if n == 0:
        return f"There are 0 matching events in the data for the question: {question}"

    # For some intents we might want extra context
    anchor = detect_anchor(question)
    wd = extract_weekday(question)
    month_info = extract_month(question)

    lines: List[str] = []

    if intent == "classes_start":
        lines.append(f"There are {n} 'Classes Begin' event row(s) for {term}:")
        for r in rows:
            lines.append(
                f"- Event '{r.get('name')}' on {r.get('start_date')} "
                f"(weekday {r.get('weekday')}), end date {r.get('end_date')}."
            )

    elif intent in ("after_anchor", "before_anchor") and anchor:
        direction = "after" if intent == "after_anchor" else "before"
        lines.append(
            f"There are {n} event row(s) in {term} that occur {direction} the anchor event '{anchor}':"
        )
        for r in rows:
            lines.append(
                f"- '{r.get('name')}' from {r.get('start_date')} to {r.get('end_date')} "
                f"(weekday {r.get('weekday')})."
            )

    elif intent == "weekday" and wd:
        lines.append(f"There are {n} event row(s) in {term} that start on {wd}:")
        for r in rows:
            lines.append(
                f"- '{r.get('name')}' on {r.get('start_date')} (ends {r.get('end_date')})."
            )

    elif intent == "month" and month_info:
        year, month = month_info
        lines.append(
            f"There are {n} event row(s) in {term} during {year}-{month:02d}:"
        )
        for r in rows:
            lines.append(
                f"- '{r.get('name')}' on {r.get('start_date')} (ends {r.get('end_date')}, weekday {r.get('weekday')})."
            )

    elif intent == "overlaps":
        lines.append(f"There are {n} overlapping event pair row(s) in {term}:")
        for r in rows:
            lines.append(
                f"- '{r.get('event1')}' ({r.get('a_start')}–{r.get('a_end')}) "
                f"overlaps with '{r.get('event2')}' ({r.get('b_start')}–{r.get('b_end')})."
            )

    elif intent == "same_day":
        lines.append(f"There are {n} same-day event pair row(s) in {term}:")
        for r in rows:
            lines.append(
                f"- On {r.get('date')}, '{r.get('event1')}' and '{r.get('event2')}' occur on the same day."
            )

    else:
        # all_events or unknown intent → list all events
        lines.append(f"There are {n} event row(s) for {term} in total:")
        for r in rows:
            lines.append(
                f"- '{r.get('name')}' from {r.get('start_date')} to {r.get('end_date')} "
                f"(weekday {r.get('weekday')})."
            )

    return "\n".join(lines)


# ---------- ANSWER FROM ROWS (LLM AS REWRITER) ----------
def answer_from_rows(
    question: str,
    rows: List[Dict[str, Any]],
    mode_desc: str,
    intent: str,
    term: str,
) -> str:
    """
    Use Neo4j rows to build a factual summary in Python.
    Then ask the LLM to rewrite that summary WITHOUT changing any facts.
    """
    if not rows:
        # If there are no rows at all, it's safe to say "nothing found".
        return f"I couldn't find any matching events for that question. ({mode_desc})"

    # Build factual summary
    summary = build_factual_summary(question, intent, term, rows)

    # Optional: table just for debugging / more transparency if needed
    headers = list(rows[0].keys())
    if HAVE_TABULATE:
        table_text = tabulate(rows, headers="keys")
    else:
        table_text = json.dumps(rows, indent=2, default=str)

    prompt = (
        "You are a precise assistant. I will give you:\n"
        "- a user question,\n"
        "- a plain factual summary that is already CORRECT, and\n"
        "- the raw table rows from Neo4j.\n\n"
        "Your job is ONLY to rewrite the factual summary into a clearer, more natural answer.\n"
        "VERY IMPORTANT RULES:\n"
        "1) Do NOT change any facts, numbers, dates, weekdays, or names.\n"
        "2) Do NOT contradict the summary (for example, if it says there are 4 events, "
        "you must not say there are 0 events).\n"
        "3) You may shorten or slightly rephrase sentences, but keep all the important details.\n"
        "4) If you are unsure, just repeat the summary exactly.\n\n"
        f"MODE DESCRIPTION: {mode_desc}\n"
        f"TERM: {term}\n\n"
        f"QUESTION:\n{question}\n\n"
        "FACTUAL SUMMARY:\n"
        f"{summary}\n\n"
        "RAW TABLE (for your reference, do not contradict it):\n"
        f"{table_text}\n\n"
        "Now rewrite the FACTUAL SUMMARY as a concise, clear answer "
        "without changing any of its facts."
    )

    return call_ollama(prompt, temperature=0.1).strip()


# ---------- MAIN CLI ----------
def main():
    if len(sys.argv) < 2:
        print('Usage: python neo4j_qa.py "Your question here"')
        sys.exit(1)

    question = " ".join(sys.argv[1:]).strip()
    print(f"QUESTION: {question}\n")

    # Decide which Cypher to run
    cypher, params, desc, intent, term = build_query_from_question(question)

    print("--- DECIDED MODE ---")
    print(desc)
    print("--------------------\n")

    print("--- CYPHER ---")
    print(cypher)
    print("--------------\n")

    # Run Cypher
    try:
        rows = run_cypher(cypher, params)
    except Exception as e:
        print("Error while running Cypher on Neo4j:", e)
        sys.exit(1)

    # Show rows
    if rows:
        print(f"--- RAW ROWS ({len(rows)}) ---")
        if HAVE_TABULATE:
            print(tabulate(rows, headers="keys"))
        else:
            print(json.dumps(rows, indent=2, default=str))
    else:
        print("--- RAW ROWS (0) ---")
    print("---------------------\n")

    # Use LLM as rewriter of our factual summary
    final = answer_from_rows(
        question=question,
        rows=rows,
        mode_desc=desc,
        intent=intent,
        term=term,
    )

    print("=== FINAL ANSWER ===")
    print(final)
    print("====================")


if __name__ == "__main__":
    main()
