SELECT coalesce(sum(coalesce(amount,0)+coalesce(length(tag),0)+pos),0) AS total FROM shape_flat
