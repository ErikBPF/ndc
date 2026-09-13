WITH leaves AS (SELECT parent_id,x3.* FROM shape_depth3_v2 LATERAL VIEW explode(children) u1 AS x1 LATERAL VIEW explode(x1.children) u2 AS x2 LATERAL VIEW explode(x2.children) u3 AS x3)
SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50;
