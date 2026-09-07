SELECT id,sort_array(collect_list(named_struct('pos',pos,'amount',amount))) selected FROM shape_flat WHERE amount>50 GROUP BY id
