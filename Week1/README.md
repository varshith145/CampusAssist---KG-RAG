# Week 1 – Neo4j Knowledge Graph Setup

This repository contains the first week’s setup for building a tiny knowledge graph in Neo4j. The example models university courses, their prerequisites, and deadlines.

## Goals of Week 1

- Install and run Neo4j locally  
- Create a small knowledge graph (Courses + Prereqs + Deadlines)  
- Run a few Cypher queries and visualize results  
- Pick an FAQ/academic page to use for data in Week 2  

---

## 1. Sanity Check

Verify that your Neo4j DBMS is running:
```cypher
RETURN 1 AS ready;
```

---

## 2. Schema Setup

Enforce uniqueness of course codes:
```cypher
CREATE CONSTRAINT course_code_unique IF NOT EXISTS
FOR (c:Course) REQUIRE c.code IS UNIQUE;
```

---

## 3. Load Sample Data

Add 5 example courses with deadlines:
```cypher
CREATE
  (c1301:Course {code:'CSCI 1301', name:'Intro to Programming', deadline: date('2025-08-15')}),
  (c1302:Course {code:'CSCI 1302', name:'Software Development',  deadline: date('2025-08-20')}),
  (c2720:Course {code:'CSCI 2720', name:'Data Structures',       deadline: date('2025-08-25')}),
  (c3030:Course {code:'CSCI 3030', name:'Computing Ethics',      deadline: date('2025-08-22')}),
  (c3360:Course {code:'CSCI 3360', name:'Database Management',   deadline: date('2025-08-30')});
```

---

## 4. Define Relationships

Connect courses to their prerequisites (direction = advanced course → prerequisite):
```cypher
MATCH
  (c1301:Course {code:'CSCI 1301'}),
  (c1302:Course {code:'CSCI 1302'}),
  (c2720:Course {code:'CSCI 2720'}),
  (c3360:Course {code:'CSCI 3360'})
CREATE
  (c1302)-[:REQUIRES]->(c1301),
  (c2720)-[:REQUIRES]->(c1302),
  (c3360)-[:REQUIRES]->(c2720);
```

---

## 5. Queries

### 5.1 Courses by upcoming deadline
```cypher
MATCH (c:Course)
RETURN c.code AS course, c.name AS name, c.deadline AS deadline
ORDER BY deadline;
```

### 5.2 Full prerequisite chain for a course
```cypher
MATCH (target:Course {code:'CSCI 3360'})
MATCH path = (target)-[:REQUIRES*]->(pre:Course)
RETURN [n IN nodes(path) | n.code] AS prereq_path
ORDER BY length(path) DESC;
```

### 5.3 Courses with no prerequisites
```cypher
MATCH (c:Course)
WHERE NOT (c)-[:REQUIRES]->(:Course)
RETURN c.code AS course, c.name AS name;
```

### 5.4 Counts (for debugging)
```cypher
MATCH (n) RETURN count(n) AS nodes;
MATCH ()-[r]->() RETURN count(r) AS relationships;
```

### 5.5 Schema visualization
```cypher
CALL db.schema.visualization();
```

### 5.6 Deadlines within 7 days
```cypher
MATCH (c:Course)
WHERE c.deadline >= date() AND c.deadline <= date() + duration('P7D')
RETURN c.code, c.deadline
ORDER BY c.deadline;
```

---

## 6. Cleanup (reset the toy graph)
```cypher
MATCH (c:Course) DETACH DELETE c;
```

---

## 7. Visualizing in Neo4j Browser

- After running queries like:
```cypher
MATCH (c:Course) RETURN c;
```
- Click the **Graph view** toggle (above the results) to see nodes and arrows.  
- Drag nodes around, zoom in/out, and hover to see properties.  

---

## Next Step (Week 2)

Pick one FAQ or academic page (e.g., UGA Registrar calendar or CS prerequisites page). Save the URL + PDF. We’ll use it to add real data into the graph.
