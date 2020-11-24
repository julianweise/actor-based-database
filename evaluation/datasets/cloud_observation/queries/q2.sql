SELECT c1.id AS c1_id, c2.id AS c2_id
FROM CloudObservation c1, CloudObservation c2
WHERE c1.seaLevelPressure < c2.seaLevelPressure
AND c1.windSpeed < c2.windSpeed
AND c1.airTemperature > c2.airTemperature
AND c1._timestamp != c2._timestamp
AND c1.solarAltitude > c2.solarAltitude
AND c1.relativeLunarIlluminance < c2.relativeLunarIlluminance
AND c1.stationElevation > c2.stationElevation
AND c1.dewPointDepression < c2.dewPointDepression
AND c1.highCloudAmount < c2.highCloudAmount
AND c1.lowerCloudAmount > c2.lowerCloudAmount
AND c1.totalCloudCover > c2.totalCloudCover
AND c1.lowerCloudBasedHeight > c2.lowerCloudBasedHeight
AND c1.middleCloudAmount > c2.middleCloudAmount;
