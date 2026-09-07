SELECT coalesce(sum(aggregate(groups,CAST(0 AS BIGINT),(a,g)->a+aggregate(g.items,CAST(0 AS BIGINT),(b,x)->b+coalesce(x.amount,0)))),0) total FROM shape
