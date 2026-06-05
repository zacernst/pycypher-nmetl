MATCH (s:State)<-[:IN]-(c:County) WITH DISTINCT id(s) AS state_name, id(c) AS county_name RETURN state_name, county_name
