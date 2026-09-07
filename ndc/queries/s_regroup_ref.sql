SELECT id,sort_array(transform(filter(items,x->x.amount>50),x->named_struct('pos',x.pos,'amount',x.amount))) selected FROM shape WHERE size(filter(items,x->x.amount>50))>0
