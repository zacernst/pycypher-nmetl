MATCH (p:Person)-[:WORKS_FOR]->(c:Company)
WITH c.name AS company, count(p) AS employee_count, avg(p.age) AS avg_age
RETURN company AS company, employee_count AS employee_count, avg_age AS avg_age
