# rag_neo4j/ask.py
from __future__ import annotations
import os, json, re, argparse
from typing import Dict, Any, List, Optional, Tuple
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tabulate import tabulate
import requests

# ---------- Config ----------
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")

HERE = os.path.dirname(__file__)
SCHEMA_PATH = os.path.join(HERE, "schema.json")
SYN_PATH = os.path.join(HERE, "synonyms.json")

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    SCHEMA = json.load(f)
with open(SYN_PATH, "r", encoding="utf-8") as f:
    SYN = json.load(f)

WEEKDAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

# ---------- Neo4j helper ----------
def run_cypher(query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as s:
            return s.run(query, params).data()
    finally:
        driver.close()

# ---------- Ollama helpers ----------
def ollama_generate(prompt: str, temperature: float = 0.2) -> str:
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "options": {"temperature": temperature}}
    with requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, stream=True, timeout=600) as r:
        r.raise_for_status()
        out = []
        for line in r.iter_lines(decode_unicode=True):
            if not line: continue
            try:
                msg = json.loads(line)
                if "response" in msg: out.append(msg["response"])
                if msg.get("done"): break
            except json.JSONDecodeError:
                # sometimes the first chunk isn't json; tolerate it
                out.append(line)
        return "".join(out)

def extract_first_json_block(text: str) -> Optional[Dict[str, Any]]:
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m: return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

# ---------- Planner (LLM → structured plan JSON) ----------
PLANNER_SYSTEM = """You are a planner that converts a user's natural-language question into a STRICT JSON plan.
The graph schema:
- Nodes:
  - (:Term { name })
  - (:Event { name, start_date, end_date, start_weekday, end_weekday, source })
- Relationship: (:Term)-[:HAS_EVENT]->(:Event)
- Dates are ISO strings YYYY-MM-DD; weekdays are Monday..Sunday.

Return a JSON with fields:
{
  "intent": "query" | "ask_clarification",
  "term": "Fall 2025" | null,
  "filters": [  // zero or more
    // allowed types:
    // {"type":"anchor_exact", "anchor_event":"Classes Begin" }
    // {"type":"after_anchor", "anchor_event":"Classes End" }
    // {"type":"before_anchor", "anchor_event":"Classes Begin" }
    // {"type":"weekday_in", "weekday":"Monday"}
    // {"type":"month_eq", "year":2025, "month":9}
    // {"type":"date_window", "start":"2025-09-01", "end":"2025-09-30"}
    // {"type":"same_day_pairs"}
    // {"type":"overlap_pairs"}
  ],
  "group_by": [],         // e.g., ["iso_year","iso_week"]
  "select": ["name","start_date","end_date","source"], // use Event fields or special keys you need
  "order_by": [ {"field":"start_date","dir":"asc"} ],
  "limit": null,
  "missing": []           // if intent=ask_clarification, list what's missing (e.g., ["term"])
}

Guidelines:
- Prefer "query" with explicit filters. Only use "ask_clarification" if key info is missing (like the term).
- Normalize casual phrases (e.g., "classes start" => anchor "Classes Begin").
- "group_by" supports: iso_year, iso_week (from start_date).
- "same_day_pairs" and "overlap_pairs" imply pairwise results.
- Do not invent data or output Cypher. Only output the JSON plan.
Strict JSON only, no prose.
"""

def normalize_anchor_phrase(text: str) -> Optional[str]:
    s = text.lower()
    # look for quoted anchor first
    m = re.search(r"'([^']+)'|\"([^\"]+)\"", text)
    if m:
        cand = (m.group(1) or m.group(2)).strip().lower()
        return SYN["anchors"].get(cand, (m.group(1) or m.group(2)).strip())
    # else try synonyms map
    for k, canon in SYN["anchors"].items():
        if k in s: return canon
    return None

def extract_term(text: str) -> Optional[str]:
    m = re.search(r"\b(Fall|Spring|Summer)\s+(\d{4})\b", text, re.IGNORECASE)
    return f"{m.group(1).title()} {m.group(2)}" if m else None

def extract_weekday(text: str) -> Optional[str]:
    s = text.lower()
    for wd in WEEKDAYS:
        if wd.lower() in s or (wd.lower()+"s") in s:
            return wd
    return None

def extract_month_year(text: str, fallback_year: Optional[int]) -> Optional[Tuple[int,int]]:
    s = text.lower()
    for name,num in MONTHS.items():
        if name in s:
            m = re.search(r"\b(20\d{2})\b", s)
            year = int(m.group(1)) if m else fallback_year
            if year is not None: return (year, num)
    return None

def plan_question_with_llm(question: str) -> Dict[str, Any]:
    # Give LLM the schema and ask for a strict plan. Prepend lightweight hints to improve reliability.
    term_hint = extract_term(question)
    wd_hint = extract_weekday(question)
    anchor_hint = normalize_anchor_phrase(question)
    my_hints = {
        "detected_term": term_hint,
        "detected_weekday": wd_hint,
        "detected_anchor": anchor_hint
    }
    prompt = PLANNER_SYSTEM + "\n\nUser question:\n" + question + "\n\nHINTS:\n" + json.dumps(my_hints) + "\n\nJSON Plan:"
    txt = ollama_generate(prompt, temperature=0.1)
    plan = extract_first_json_block(txt) or {}
    # Minimal fallback: if LLM failed, try to construct a tiny plan with just term & trivial filter
    if not plan:
        term = term_hint
        if not term:
            return {"intent":"ask_clarification","missing":["term"]}
        return {"intent":"query","term":term,"filters":[],"group_by":[],"select":["name","start_date","end_date","source"],"order_by":[{"field":"start_date","dir":"asc"}],"limit":None}
    return plan

# ---------- Validator ----------
ALLOWED_FILTERS = set(SCHEMA["allow_filters"])
ALLOWED_GROUP = set(SCHEMA["allow_group_by"])
ALLOWED_ORDER = set(SCHEMA["allow_order_by"])

def validate_and_fix_plan(plan: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    # returns (fixed_plan, error_message_if_any)
    if "intent" not in plan:
        return None, "Planner did not return an intent."
    if plan["intent"] == "ask_clarification":
        return plan, None

    # Ensure term
    term = plan.get("term")
    if not term:
        return {"intent":"ask_clarification","missing":["term"]}, None

    # Filters
    filters = plan.get("filters", [])
    fixed_filters = []
    for f in filters:
        t = f.get("type")
        if t not in ALLOWED_FILTERS:
            # drop unknown filters
            continue
        # normalize anchors if present
        if "anchor_event" in f and f["anchor_event"]:
            norm = SYN["anchors"].get(f["anchor_event"].lower(), f["anchor_event"])
            f["anchor_event"] = norm
        fixed_filters.append(f)

    # Group by
    group_by = [g for g in (plan.get("group_by") or []) if g in ALLOWED_GROUP]

    # Order by
    order_by = []
    for ob in (plan.get("order_by") or []):
        fld = ob.get("field")
        if fld in ALLOWED_ORDER:
            order_by.append({"field": fld, "dir": (ob.get("dir","asc").lower() if ob.get("dir") else "asc")})

    select = plan.get("select") or ["name","start_date","end_date","source"]
    fixed = {
        "intent":"query",
        "term": term,
        "filters": fixed_filters,
        "group_by": group_by,
        "select": select,
        "order_by": order_by,
        "limit": plan.get("limit")
    }
    return fixed, None

# ---------- Cypher builder (from plan) ----------
def cypher_from_plan(plan: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    term = plan["term"]
    filters = plan["filters"]
    where_clauses = []
    params: Dict[str, Any] = {"term": term}

    # always match term + events
    prelude = "MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)\n"

    # handle anchor filters (need an earlier match)
    anchor_needed = any(f["type"] in ("after_anchor","before_anchor","anchor_exact") for f in filters)
    if anchor_needed:
        # There can be more than one, we take the first for anchor_date/anchor_name context
        f0 = next(f for f in filters if f["type"] in ("after_anchor","before_anchor","anchor_exact"))
        anchor = f0["anchor_event"]
        params["anchor"] = anchor
        prelude = (
            "MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event {name:$anchor})\n"
            "WITH a.start_date AS anchor_date\n"
            "MATCH (:Term {name:$term})-[:HAS_EVENT]->(e:Event)\n"
        )
        # add where pieces for after/before
        for f in filters:
            if f["type"] == "after_anchor":
                where_clauses.append("e.start_date > anchor_date")
            elif f["type"] == "before_anchor":
                where_clauses.append("e.start_date < anchor_date")
            elif f["type"] == "anchor_exact":
                where_clauses.append("e.name = $anchor")  # if they ask for just the anchor

    # non-anchor filters
    for f in filters:
        t = f["type"]
        if t == "weekday_in":
            params["weekday"] = f["weekday"]
            where_clauses.append("e.start_weekday = $weekday")
        elif t == "month_eq":
            y, m = f["year"], f["month"]
            params["start"] = f"{y:04d}-{m:02d}-01"
            if m == 12:
                params["end"] = f"{y+1:04d}-01-01"
            else:
                params["end"] = f"{y:04d}-{m+1:02d}-01"
            where_clauses.append("e.start_date >= date($start) AND e.start_date < date($end)")
        elif t == "date_window":
            params["win_start"] = f["start"]
            params["win_end"] = f["end"]
            where_clauses.append("e.start_date >= date($win_start) AND e.start_date <= date($win_end)")
        elif t in ("same_day_pairs","overlap_pairs"):
            # handled separately below as pair queries
            pass

    where_str = ("WHERE " + " AND ".join(where_clauses) + "\n") if where_clauses else ""

    # pair queries (special shape)
    if any(f["type"] == "same_day_pairs" for f in filters):
        q = (
            "MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),\n"
            "      (:Term {name:$term})-[:HAS_EVENT]->(b:Event)\n"
            "WHERE a.start_date = b.start_date AND id(a) < id(b)\n"
            "RETURN a.name AS event1, toString(a.start_date) AS date, b.name AS event2\n"
            "ORDER BY date\n"
        )
        return q, {"term": term}

    if any(f["type"] == "overlap_pairs" for f in filters):
        q = (
            "MATCH (:Term {name:$term})-[:HAS_EVENT]->(a:Event),\n"
            "      (:Term {name:$term})-[:HAS_EVENT]->(b:Event)\n"
            "WHERE a.start_date <= b.end_date AND b.start_date <= a.end_date AND id(a) < id(b)\n"
            "RETURN a.name AS event1, toString(a.start_date) AS a_start, toString(a.end_date) AS a_end,\n"
            "       b.name AS event2, toString(b.start_date) AS b_start, toString(b.end_date) AS b_end\n"
            "ORDER BY a_start, b_start\n"
        )
        return q, {"term": term}

    # standard event rows
    select_cols = plan.get("select") or ["name","start_date","end_date","source"]
    return_cols = []
    for c in select_cols:
        if c == "name":
            return_cols.append("e.name AS name")
        elif c in ("start_date","end_date"):
            return_cols.append(f"toString(e.{c}) AS {c}")
        elif c == "source":
            return_cols.append("e.source AS source")

    # group_by adds iso fields
    group_by = plan.get("group_by") or []
    if "iso_week" in group_by:
        return_cols.append("e.start_date.week AS iso_week")
    if "iso_year" in group_by:
        return_cols.append("e.start_date.year AS iso_year")

    ret = "RETURN " + ", ".join(return_cols) + "\n"

    # order by (default start_date asc)
    order_by = plan.get("order_by") or [{"field":"start_date","dir":"asc"}]
    ob = []
    for item in order_by:
        fld = item["field"]; direc = item.get("dir","asc").upper()
        if fld in ("start_date","end_date"):
            ob.append(f"e.{fld} {direc}")
        elif fld == "name":
            ob.append("e.name " + direc)
    order = "ORDER BY " + ", ".join(ob) + "\n" if ob else ""

    q = prelude + where_str + ret + order
    return q, params

# ---------- Answerer (LLM writes the final answer, grounded) ----------
def answer_from_rows(question: str, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "I couldn't find matching events for that question in the graph."

    headers = list(rows[0].keys())
    table_text = tabulate([[r.get(h) for h in headers] for r in rows], headers=headers)
    prompt = (
        "You are a precise assistant. Answer the user's question ONLY using the TABLE below.\n"
        "Be concise, include dates, and avoid any facts not present in the table.\n\n"
        f"QUESTION:\n{question}\n\nTABLE:\n{table_text}\n\n"
        "Write the final answer now."
    )
    return ollama_generate(prompt, temperature=0.2).strip()

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Neo4j-only NL→Plan→Cypher→Answer")
    ap.add_argument("question", nargs="+", help="Ask in natural language")
    ap.add_argument("--debug", action="store_true", help="Print plan, Cypher and row count")
    args = ap.parse_args()

    user_q = " ".join(args.question)

    # 1) Get plan from planner LLM
    raw_plan = plan_question_with_llm(user_q)
    fixed_plan, err = validate_and_fix_plan(raw_plan)
    if err:
        print("Planner error:", err); return
    if fixed_plan["intent"] == "ask_clarification":
        missing = fixed_plan.get("missing", ["term"])
        print(f"Need more info: please specify {', '.join(missing)} (e.g., 'Fall 2025').")
        return

    # 2) Build Cypher safely from plan
    cypher, params = cypher_from_plan(fixed_plan)

    # 3) Run Cypher
    rows = run_cypher(cypher, params)

    if args.debug:
        print("\n--- PLAN ---")
        print(json.dumps(fixed_plan, indent=2))
        print("\n--- CYPHER ---")
        print(cypher)
        print("\n--- ROWS ---")
        print(f"{len(rows)} row(s)")

    # 4) Always produce an LLM-written answer (grounded)
    final = answer_from_rows(user_q, rows)

    # Print both: concise answer + (optional) a small table for transparency
    print("\n" + final.strip())
    if rows:
        # If grouped is requested, show grouped blocks; else show a top table
        if set(fixed_plan.get("group_by") or []) & {"iso_week","iso_year"}:
            print("\n(Results grouped by ISO week)")
            groups: Dict[Tuple[int,int], List[Dict[str,Any]]] = {}
            for r in rows:
                year = r.get("iso_year"); week = r.get("iso_week")
                groups.setdefault((year, week), []).append(r)
            for (y,w) in sorted(groups.keys()):
                print(f"\nWeek {w}, {y}")
                block = groups[(y,w)]
                print(tabulate([{k:v for k,v in rr.items() if k not in ("iso_year","iso_week")} for rr in block], headers="keys"))
        else:
            print("\n(Results)")
            print(tabulate(rows, headers="keys"))

if __name__ == "__main__":
    main()
