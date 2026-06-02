MATCH (p:Person)-[:WORKS_AT]->(c:Company) WITH p.age + 1 AS age_plus_one, p.name AS name, c.name AS company_name RETURN age_plus_one, name, company_name
