SELECT id FROM shape WHERE exists(items,x->x.amount>50) AND NOT exists(items,x->coalesce(x.amount<10,false))
