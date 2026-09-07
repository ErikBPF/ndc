SELECT id,filter(items,x->x IS NOT NULL) items FROM shape WHERE size(filter(items,x->x IS NOT NULL))>0
