MATCH (p:Person)-[:LIVES_IN]->(c:City) WITH c.city_name AS city_name, COUNT(p) AS num_people RETURN city_name, num_people
