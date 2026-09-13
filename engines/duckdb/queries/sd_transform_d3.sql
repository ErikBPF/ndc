WITH leaves AS (SELECT parent_id,x3.* FROM shape_depth3_v2 CROSS JOIN UNNEST(children) u1(x1) CROSS JOIN UNNEST(x1.children) u2(x2) CROSS JOIN UNNEST(x2.children) u3(x3))
SELECT parent_id,list(struct_pack(leaf_id:=leaf_id,adjusted:=amount*2,tag:=tag,padding:=padding) ORDER BY leaf_id) AS selected FROM leaves WHERE amount>=50 GROUP BY parent_id ORDER BY parent_id;
