SELECT MAX(y.v1, y.v2) FROM y;
SELECT (y.v1 + y.v2) / 2 FROM y;
SELECT MIN(y.v1, y.v2) FROM y;

SELECT SUM(x.v1 * agg.max) / (SQRT(SUM(POWER(x.v1, 2))) * SQRT(SUM(POWER(agg.max, 2))))
FROM x
JOIN (
    SELECT MAX(y.v1, y.v2) as max, y.date
    FROM y
    ) as agg
    ON x.date=agg.date;

SELECT SUM(x.v1 * agg.avg) / (SQRT(SUM(POWER(x.v1, 2))) * SQRT(SUM(POWER(agg.avg, 2))))
FROM x
JOIN (
    SELECT (y.v1 + y.v2) / 2 as avg, y.date
    FROM y
    ) as agg
    ON x.date=agg.date;

SELECT SUM(x.v1 * agg.min) / (SQRT(SUM(POWER(x.v1, 2))) * SQRT(SUM(POWER(agg.min, 2))))
FROM x
JOIN (
    SELECT MIN(y.v1, y.v2) as min, y.date
    FROM y
    ) as agg
    ON x.date=agg.date;


-- Mock; more concrete implementation
-- SELECT * FROM
--     (SELECT stock, station,
--             SUM(x.v1 * agg.max) / (SQRT(SUM(POWER(x.v1, 2))) * SQRT(SUM(POWER(agg.max, 2)))) as cos
--      FROM x
--      JOIN (
--          SELECT MAX(y.v1, y.v2) as max, y.date
--          FROM y
--          ) as agg
--      ON x.date=agg.date
--      GROUP BY stock, station)
-- WHERE cos > tau; 
