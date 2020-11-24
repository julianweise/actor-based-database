SELECT l1.id AS l1_id, l2.id AS l2_id
FROM line_and_part l1, line_and_part l2
WHERE l1.p_retailprice = l2.l_extendedprice
AND l1.l_quantity = l2.l_quantity;
