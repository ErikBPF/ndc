SELECT coalesce(sum(aggregate(arrays_zip(amounts,tags),CAST(0 AS BIGINT),(a,x)->a+coalesce(x.amounts,0)+coalesce(length(x.tags),0))),0) total FROM shape
