SELECT l1.id AS l1_id, l2.id AS l2_id
FROM line_and_part l1, line_and_part l2
WHERE l1.l_extendedprice = l2.l_extendedprice
AND l1.p_retailprice = l2.p_retailprice
AND l1.l_quantity = l2.l_quantity
AND l1.l_tax = l2.l_tax
AND l1.l_discount = l2.l_discount;
