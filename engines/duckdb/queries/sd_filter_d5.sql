WITH leaves AS (SELECT parent_id,x5.* FROM shape_depth5_v2 CROSS JOIN UNNEST(children) u1(x1) CROSS JOIN UNNEST(x1.children) u2(x2) CROSS JOIN UNNEST(x2.children) u3(x3) CROSS JOIN UNNEST(x3.children) u4(x4) CROSS JOIN UNNEST(x4.children) u5(x5))
SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50;
