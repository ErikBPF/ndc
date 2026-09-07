SELECT coalesce(sum(coalesce(amount,0)+coalesce(length(tag),0)),0) total FROM shape_flat
