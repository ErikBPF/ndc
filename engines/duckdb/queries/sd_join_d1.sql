WITH leaves AS (SELECT parent_id,x1.* FROM shape_depth1_v2 CROSS JOIN UNNEST(children) u1(x1))
SELECT p.bucket,count(*) AS item_count,CAST(sum(l.amount) AS BIGINT) AS total FROM leaves l JOIN shape_parents_v2 p ON l.parent_id=p.parent_id WHERE l.amount IS NOT NULL GROUP BY p.bucket ORDER BY p.bucket;
