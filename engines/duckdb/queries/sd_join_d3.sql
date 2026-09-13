WITH leaves AS (SELECT parent_id,x3.* FROM shape_depth3_v2 CROSS JOIN UNNEST(children) u1(x1) CROSS JOIN UNNEST(x1.children) u2(x2) CROSS JOIN UNNEST(x2.children) u3(x3))
SELECT p.bucket,count(*) AS item_count,CAST(sum(l.amount) AS BIGINT) AS total FROM leaves l JOIN shape_parents_v2 p ON l.parent_id=p.parent_id WHERE l.amount IS NOT NULL GROUP BY p.bucket ORDER BY p.bucket;
