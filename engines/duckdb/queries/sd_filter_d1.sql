WITH leaves AS (SELECT parent_id,x1.* FROM shape_depth1_v2 CROSS JOIN UNNEST(children) u1(x1))
SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50;
