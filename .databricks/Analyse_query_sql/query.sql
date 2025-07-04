
-- Longest runway per country

SELECT country_name, airport_name, length_ft, width_ft
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY length_ft DESC) AS rn
  FROM assignment.curated.airport_runway_country
) WHERE rn = 1;


-- Shortest runway per country

SELECT country_name, airport_name, length_ft, width_ft
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY length_ft ASC) AS rn
  FROM assignment.curated.airport_runway_country
) WHERE rn = 1;



-- Top 3 countries that have the most airports

SELECT *
FROM assignment.curated.airport_counts_per_country
ORDER BY airport_count DESC
LIMIT 3;



-- Bottom 10 countries that have least number of airports

SELECT *
FROM assignment.curated.airport_counts_per_country
WHERE airport_count > 0
ORDER BY airport_count ASC
LIMIT 10;
