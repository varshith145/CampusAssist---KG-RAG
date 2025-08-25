// --- Week 1 Neo4j Setup ---

// Sanity check
RETURN 1 AS ready;

// Schema: enforce unique course codes
CREATE CONSTRAINT course_code_unique IF NOT EXISTS
FOR (c:Course) REQUIRE c.code IS UNIQUE;

// Load sample courses
CREATE
  (c1301:Course {code:'CSCI 1301', name:'Intro to Programming', deadline: date('2025-08-15')}),
  (c1302:Course {code:'CSCI 1302', name:'Software Development',  deadline: date('2025-08-20')}),
  (c2720:Course {code:'CSCI 2720', name:'Data Structures',       deadline: date('2025-08-25')}),
  (c3030:Course {code:'CSCI 3030', name:'Computing Ethics',      deadline: date('2025-08-22')}),
  (c3360:Course {code:'CSCI 3360', name:'Database Management',   deadline: date('2025-08-30')});

// Define prerequisite relationships (advanced → prerequisite)
MATCH
  (c1301:Course {code:'CSCI 1301'}),
  (c1302:Course {code:'CSCI 1302'}),
  (c2720:Course {code:'CSCI 2720'}),
  (c3360:Course {code:'CSCI 3360'})
CREATE
  (c1302)-[:REQUIRES]->(c1301),
  (c2720)-[:REQUIRES]->(c1302),
  (c3360)-[:REQUIRES]->(c2720);

// --- Queries ---

// Courses by upcoming deadline
MATCH (c:Course)
RETURN c.code AS course, c.name AS name, c.deadline AS deadline
ORDER BY deadline;

// Full prerequisite chain for Database Management
MATCH (target:Course {code:'CSCI 3360'})
MATCH path = (target)-[:REQUIRES*]->(pre:Course)
RETURN [n IN nodes(path) | n.code] AS prereq_path
ORDER BY length(path) DESC;

// Courses with no prerequisites
MATCH (c:Course)
WHERE NOT (c)-[:REQUIRES]->(:Course)
RETURN c.code AS course, c.name AS name;

// Count nodes and relationships
MATCH (n) RETURN count(n) AS nodes;
MATCH ()-[r]->() RETURN count(r) AS relationships;

// Visualize schema
CALL db.schema.visualization();

// Courses with deadlines in the next 7 days
MATCH (c:Course)
WHERE c.deadline >= date() AND c.deadline <= date() + duration('P7D')
RETURN c.code, c.deadline
ORDER BY c.deadline;

// --- Cleanup (reset the graph) ---
MATCH (c:Course) DETACH DELETE c;
