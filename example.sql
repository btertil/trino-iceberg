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

-- new table

create table if not exists iceberg.sales.sales_rep_ranked as
    SELECT
        sales_person_name,
        count(*) ile,
        avg(sales_id) avg_score,
        sum(sales_id) total_score,
        rank() OVER (ORDER BY sum(sales_id) DESC) rank
    FROM iceberg.sales.sales_rep GROUP BY sales_person_name;

-- statistics for query optimization
analyze iceberg.sales.sales_rep_ranked;

SELECT * FROM iceberg.sales.sales_rep_ranked;

-- some examples 
show catalogs;

show schemas in iceberg;
show schemas from iceberg;

show tables in iceberg.sales;
show tables from iceberg.sales;

-- session information
show session;
show session like 'dist%';
-- set session dist_sql_planner_iceberg_optimized_joins = true;
show roles;
show current roles;

select 'Today is ' || cast(current_date as varchar);

use iceberg.sales;
select * from sales_rep;

show columns from sales_rep_ranked;

-- AVRO
CREATE TABLE iceberg.sales.sales_rep_avro (
  sales_id BIGINT,
  sales_person_name VARCHAR
)
WITH (format = 'AVRO');

INSERT INTO iceberg.sales.sales_rep_avro VALUES
(3, 'X'),
(4, 'Y'),
(25, 'X'),
(4, 'Y'),
(21, 'X'),
(23, 'X');

analyze iceberg.sales.sales_rep_avro;

SELECT * FROM iceberg.sales.sales_rep_avro;

SELECT
    sales_person_name,
    count(*) ile,
    avg(sales_id) avg_score,
    sum(sales_id) total_score,
    rank() OVER (ORDER BY sum(sales_id) DESC) rank
FROM iceberg.sales.sales_rep_avro GROUP BY sales_person_name;
