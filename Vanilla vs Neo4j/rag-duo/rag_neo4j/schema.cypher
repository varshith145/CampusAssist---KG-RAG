// Unique term names
CREATE CONSTRAINT term_name_unique IF NOT EXISTS
FOR (t:Term) REQUIRE t.name IS UNIQUE;

// Helpful indexes
CREATE INDEX event_name_index IF NOT EXISTS
FOR (e:Event) ON (e.name);

CREATE INDEX event_start_date_index IF NOT EXISTS
FOR (e:Event) ON (e.start_date);

CREATE INDEX event_end_date_index IF NOT EXISTS
FOR (e:Event) ON (e.end_date);
