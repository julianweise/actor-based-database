SELECT a.id AS a_id, b.id AS b_id
FROM flight a, flight b
WHERE a.crs_elapsed_time > b.crs_elapsed_time
AND a.actual_elapsed_time < b.actual_elapsed_time
AND a.arr_delay >= b.arr_delay
AND a.dep_delay <= b.dep_delay;
