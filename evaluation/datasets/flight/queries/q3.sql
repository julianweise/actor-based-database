SELECT a.id AS a_id, b.id AS b_id
FROM flight a, flight b
WHERE a.crs_elapsed_time < b.arr_delay
AND a.distance > b.distance
AND a.dep_delay > b.dep_delay
AND a.actual_elapsed_time > b.actual_elapsed_time
AND a.taxi_in < b.taxi_in;
