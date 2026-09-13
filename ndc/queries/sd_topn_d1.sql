WITH leaves AS (SELECT parent_id,x1.* FROM shape_depth1_v2 LATERAL VIEW explode(children) u1 AS x1)
SELECT parent_id,CAST(sum(amount) AS BIGINT) AS total FROM (SELECT parent_id,amount,row_number() OVER (PARTITION BY parent_id ORDER BY amount DESC,leaf_id) AS rn FROM leaves WHERE amount IS NOT NULL) ranked WHERE rn<=2 GROUP BY parent_id ORDER BY parent_id;
