WITH leaves AS (SELECT * FROM shape_leaves_v2)
SELECT parent_id,list(struct_pack(leaf_id:=leaf_id,adjusted:=amount*2,tag:=tag,padding:=padding) ORDER BY leaf_id) AS selected FROM leaves WHERE amount>=50 GROUP BY parent_id ORDER BY parent_id;
