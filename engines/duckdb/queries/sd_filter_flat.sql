WITH leaves AS (SELECT * FROM shape_leaves_v2)
SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50;
