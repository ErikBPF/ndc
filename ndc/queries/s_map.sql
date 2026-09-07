SELECT coalesce(sum(size(filter(items,x->x.attrs['source']='channel1'))),0) total FROM shape WHERE items IS NOT NULL
