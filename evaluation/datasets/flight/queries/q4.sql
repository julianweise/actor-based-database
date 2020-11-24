SELECT a.id AS a_id, b.id AS b_id
FROM flight a, flight b
WHERE a.dep_delay > b.distance
AND a.actual_elapsed_time < b.actual_elapsed_time
AND a.arr_delay < b.arr_delay
AND a.taxi_in > b.taxi_in;

