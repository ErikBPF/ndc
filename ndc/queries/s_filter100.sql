SELECT coalesce(sum(aggregate(filter(items,x->x.amount<100),CAST(0 AS BIGINT),(a,x)->a+x.amount)),0) total FROM shape
