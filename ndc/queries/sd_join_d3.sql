WITH leaves AS (SELECT parent_id,x3.* FROM shape_depth3_v2 LATERAL VIEW explode(children) u1 AS x1 LATERAL VIEW explode(x1.children) u2 AS x2 LATERAL VIEW explode(x2.children) u3 AS x3)
SELECT p.bucket,count(*) AS item_count,CAST(sum(l.amount) AS BIGINT) AS total FROM leaves l JOIN shape_parents_v2 p ON l.parent_id=p.parent_id WHERE l.amount IS NOT NULL GROUP BY p.bucket ORDER BY p.bucket;
