WITH leaves AS (SELECT parent_id,x5.* FROM shape_depth5_v2 LATERAL VIEW explode(children) u1 AS x1 LATERAL VIEW explode(x1.children) u2 AS x2 LATERAL VIEW explode(x2.children) u3 AS x3 LATERAL VIEW explode(x3.children) u4 AS x4 LATERAL VIEW explode(x4.children) u5 AS x5)
SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50;
