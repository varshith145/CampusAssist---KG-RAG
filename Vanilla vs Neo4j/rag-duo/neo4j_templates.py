# neo4j_templates.py
from __future__ import annotations
from typing import Dict, Any, List
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# ---- Neo4j helper ----
def run_cypher(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as s:
            return s.run(query, params or {}).data()
    finally:
        driver.close()

# ---- Cypher templates (deterministic) ----
def t_classes_begin_date(term: str) -> List[Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event {name:'Classes Begin'})
    RETURN e.name AS name, toString(e.start_date) AS start_date, toString(e.end_date) AS end_date, e.source AS source
    LIMIT 1
    """
    return run_cypher(q, {"term": term})

def t_after_begin(term: str) -> List[Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(cb:Event {name:'Classes Begin'})
    WITH cb.start_date AS anchor
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date > anchor
    RETURN e.name AS name, toString(e.start_date) AS start_date, toString(e.end_date) AS end_date, e.source AS source
    ORDER BY e.start_date
    """
    return run_cypher(q, {"term": term})

def t_weekday(term: str, weekday: str) -> List[Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_weekday = $weekday
    RETURN e.name AS name, toString(e.start_date) AS start_date, toString(e.end_date) AS end_date, e.source AS source
    ORDER BY e.start_date
    """
    return run_cypher(q, {"term": term, "weekday": weekday})

def t_month(term: str, year: int, month: int) -> List[Dict[str, Any]]:
    # 1 <= month <= 12
    start = f"{year:04d}-{month:02d}-01"
    # naive end: next month 01
    if month == 12:
        end = f"{year+1:04d}-01-01"
    else:
        end = f"{year:04d}-{month+1:02d}-01"
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date >= date($start) AND e.start_date < date($end)
    RETURN e.name AS name, toString(e.start_date) AS start_date, toString(e.end_date) AS end_date, e.source AS source
    ORDER BY e.start_date
    """
    return run_cypher(q, {"term": term, "start": start, "end": end})

def t_window(term: str, start: str, end: str) -> List[Dict[str, Any]]:
    # start/end = 'YYYY-MM-DD'
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)
    WHERE e.start_date >= date($start) AND e.start_date <= date($end)
    RETURN e.name AS name, toString(e.start_date) AS start_date, toString(e.end_date) AS end_date, e.source AS source
    ORDER BY e.start_date
    """
    return run_cypher(q, {"term": term, "start": start, "end": end})

def t_same_day(term: str) -> List[Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),
          (:Term {name:$term})-[:HAS_EVENT]->(b:Event)
    WHERE a.start_date = b.start_date AND id(a) < id(b)
    RETURN a.name AS event1, b.name AS event2, toString(a.start_date) AS date
    ORDER BY date
    """
    return run_cypher(q, {"term": term})

def t_overlaps(term: str) -> List[Dict[str, Any]]:
    q = """
    MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),
          (:Term {name:$term})-[:HAS_EVENT]->(b:Event)
    WHERE a.start_date <= b.end_date AND b.start_date <= a.end_date AND id(a) < id(b)
    RETURN a.name AS event1, toString(a.start_date) AS a_start, toString(a.end_date) AS a_end,
           b.name AS event2, toString(b.start_date) AS b_start, toString(b.end_date) AS b_end
    ORDER BY a_start, b_start
    """
    return run_cypher(q, {"term": term})

# ---- Template registry (for router to call) ----
TEMPLATES = {
    "classes-begin-date": {"fn": t_classes_begin_date, "params": ["term"]},
    "after-begin":        {"fn": t_after_begin,        "params": ["term"]},
    "weekday":            {"fn": t_weekday,            "params": ["term", "weekday"]},
    "month":              {"fn": t_month,              "params": ["term", "year", "month"]},
    "window":             {"fn": t_window,             "params": ["term", "start", "end"]},
    "same-day":           {"fn": t_same_day,           "params": ["term"]},
    "overlaps":           {"fn": t_overlaps,           "params": ["term"]},
}
