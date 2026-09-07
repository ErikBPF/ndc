WITH totals AS (SELECT bucket,sum(amount) total FROM shape_flat GROUP BY bucket) SELECT bucket,total,dense_rank() OVER (ORDER BY total DESC) rank FROM totals
