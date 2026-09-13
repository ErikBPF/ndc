WITH leaves AS (SELECT * FROM shape_leaves_v2)
SELECT parent_id,sort_array(collect_list(named_struct('leaf_id',leaf_id,'adjusted',amount*2,'tag',tag,'padding',padding))) AS selected FROM leaves WHERE amount>=50 GROUP BY parent_id ORDER BY parent_id;
