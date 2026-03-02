CREATE SCHEMA IF NOT EXISTS iceberg.sales
WITH (location = 's3://warehouse/sales');

CREATE TABLE iceberg.sales.sales_rep (
  sales_id BIGINT,
  sales_person_name VARCHAR
)
WITH (format = 'PARQUET');

INSERT INTO iceberg.sales.sales_rep VALUES
(1, 'X'),
(2, 'Y');

SELECT * FROM iceberg.sales.sales_rep;


-- some more rows

INSERT INTO iceberg.sales.sales_rep VALUES
(3, 'X'),
(4, 'Y'),
(25, 'X'),
(4, 'Y'),
(21, 'X'),
(23, 'X');

SELECT * FROM iceberg.sales.sales_rep;

-- analysis

SELECT
    sales_person_name,
    count(*) ile,
    avg(sales_id) avg_score,
    sum(sales_id) total_score,
    rank() OVER (ORDER BY sum(sales_id) DESC) rank
FROM iceberg.sales.sales_rep GROUP BY sales_person_name;

-- views are not supported in iceberg, but we can create a view in trino and query it

-- create view iceberg.sales.v_sales_rep_scores_rank as
--     SELECT
--         sales_person_name,
--         count(*) ile,
--         avg(sales_id) avg_score,
--         sum(sales_id) total_score,
--         rank() OVER (ORDER BY sum(sales_id) DESC) rank
--     FROM iceberg.sales.sales_rep GROUP BY sales_person_name;

-- select * from iceberg.sales.v_sales_rep_scores_rank;
