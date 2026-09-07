SELECT id,named_struct('amount',id*10,'items',filter(items,x->x.amount>=50)) payload FROM shape
