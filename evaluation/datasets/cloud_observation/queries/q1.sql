SELECT c1.id AS c1_id, c2.id AS c2_id
FROM CloudObservation c1, CloudObservation c2
WHERE c1.totalCloudCover > c2.totalCloudCover
AND c1.lowerCloudAmount < c2.lowerCloudAmount
AND c1.middleCloudAmount < c2.middleCloudAmount
AND c1.highCloudAmount > c2.highCloudAmount
AND c1.nonOverheadHighCloudAmount < c2.nonOverheadHighCloudAmount;
