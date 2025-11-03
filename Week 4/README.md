# ETL + Neo4j Import (UGA Academic Calendars)

This guide explains how to run **`etl_and_import.py`** which:
1) Scrapes the UGA Academic Calendars (Fall 2025, Spring 2026)
2) Creates a CSV (`data/events.csv`)
3) Imports the data into Neo4j automatically

You can choose **one of two import methods**:
- **Driver mode (recommended):** Inserts rows directly using the Neo4j Python driver (no need to locate the Desktop `import` folder).
- **LOAD CSV mode (optional):** Copies the CSV into the DB’s `import` folder and runs `LOAD CSV` inside Neo4j.

---

## Prerequisites

- **Neo4j Desktop** installed and a database running (note the username/password).
- **Python 3.8+**
- Install required Python packages:
  ```bash
  pip install requests beautifulsoup4 neo4j
  ```

> Tip: Make sure your Neo4j DB is **started** before running the script.  
> Default local Bolt URL is `bolt://localhost:7687`.

---

## How to Run (Driver Mode — Recommended)

This is the simplest, most reliable path. It connects via Bolt and writes nodes directly.

```bash
python etl_and_import.py --method driver \
  --bolt bolt://localhost:7687 \
  --user neo4j \
  --password YOUR_PASSWORD \
  --db neo4j
```

**What happens:**
- Scrapes the Registrar page
- Writes `data/events.csv`
- Upserts `:Event` nodes with properties: `term`, `event`, `date`, `source`
- Adds a node key constraint `(term,event,date)` to prevent duplicates
- Adds small indexes on `term` and `event` for faster lookups

---

## How to Run (LOAD CSV Mode — Optional)

Use this if your instructor specifically wants `LOAD CSV` from the `import` folder.

1) In Neo4j Desktop → open your database → **Files** → **Import** → **Open**.  
   Copy that folder path (e.g., `C:\Users\Owner\.Neo4j\relate-data\dbmss\<dbms-id>\import`).  
   If `import` doesn’t exist, create it.

2) Run the script:
```bash
python etl_and_import.py --method loadcsv \
  --import-dir "C:\Users\Owner\.Neo4j\relate-data\dbmss\<your-dbms-id>\import" \
  --bolt bolt://localhost:7687 \
  --user neo4j \
  --password YOUR_PASSWORD \
  --db neo4j \
  --csv-name events.csv
```

**What happens:**
- Writes `data/events.csv` (artifact for your report)
- Copies it into the Desktop `import` folder
- Executes `LOAD CSV` to create/merge `:Event` nodes
- Ensures the same schema constraints/indexes as in driver mode

---

## Verifying the Import (Run in Neo4j Browser)

```cypher
// Total count
MATCH (e:Event) RETURN count(e);

// Distinct terms
MATCH (e:Event) RETURN DISTINCT e.term ORDER BY e.term;

// Sample query
MATCH (e:Event)
WHERE e.term = 'Spring 2026' AND e.event CONTAINS 'Classes Begin'
RETURN e.event, e.date;

// Keyword search
MATCH (e:Event)
WHERE toLower(e.event) CONTAINS 'final'
RETURN e.term, e.event, e.date
ORDER BY e.term;
```

---

## Common Issues & Fixes

- **"Could not load external resource"** (only in LOAD CSV mode):  
  The CSV is not inside the DB’s `import` folder. Put `events.csv` there and use `file:///events.csv`.

- **No results showing:**  
  Run one query at a time in Browser (Neo4j doesn’t execute multiple statements at once in the Browser editor).

- **Duplicates after re-running:**  
  The script creates a **node key** on `(term,event,date)`, so re-runs won’t duplicate.  
  If you want a clean slate anyway:
  ```cypher
  MATCH (e:Event) DETACH DELETE e;
  ```

- **Forgot password / DB not running:**  
  Start the DB in Desktop and verify the Bolt URL, user, and password. Reset password from Desktop if needed.

---

## File Outputs

- **CSV:** `data/events.csv`
- **Nodes created:** label `:Event` with properties `term`, `event`, `date`, `source`

---

## Notes / Suggestions

- For future years, adjust the `TERMS` list inside `etl_and_import.py`.  
- Keep `driver` mode for new/unknown Desktop instances; it avoids path problems.  
- If you need to sort by date, consider creating a `sort_key`:
  ```cypher
  MATCH (e:Event)
  WITH e, split(e.date, ' to ')[0] AS start
  SET e.sort_key = substring(start,6,4) + substring(start,0,2) + substring(start,3,2);
  ```


