SELECT a.id AS a_id, b.id AS b_id
FROM flight a, flight b
WHERE a.TAXI_IN > b.ACTUAL_ELAPSED_TIME
AND a.ARR_DELAY < b.ARR_DELAY
AND a.DISTANCE < b.DISTANCE;
