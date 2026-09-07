SELECT coalesce(sum(aggregate(items,CAST(0 AS BIGINT),(a,x)->a+coalesce(x.amount,0)+coalesce(length(x.tag),0)+coalesce(x.pos,0))),0) AS total FROM shape
