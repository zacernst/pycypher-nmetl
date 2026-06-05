MATCH (s)<-[:IN]-(c:COUNTY) WITH DISTINCT id(s) AS state_name, COUNT(c) AS num_counties RETURN state_name, num_counties
