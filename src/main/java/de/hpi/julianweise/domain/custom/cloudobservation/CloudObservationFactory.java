package de.hpi.julianweise.domain.custom.cloudobservation;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

public class CloudObservationFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return CloudObservation.class;
    }

    @Override
    public ADBEntity build(Record record) {
        return CloudObservation.builder()
                               ._timestamp(record.getInt(0))
                               .skyBrightnessIndicator(record.getString(1).equals("1"))
                               .latitude(record.getShort(2))
                               .longitude(record.getInt(3))
                               .stationNumber(record.getInt(4))
                               .landIndicator(record.getString(5).equals("1"))
                               .presentWeather(record.getByte(6))
                               .totalCloudCover(record.getByte(7))
                               .lowerCloudAmount(record.getByte(8))
                               .lowerCloudBasedHeight(record.getByte(9))
                               .lowCloudType(record.getByte(10))
                               .middleCloudType(record.getByte(11))
                               .highCloudType(record.getByte(12))
                               .middleCloudAmount(record.getShort(13))
                               .highCloudAmount(record.getShort(14))
                               .nonOverheadMiddleCloudAmount(record.getByte(15))
                               .nonOverheadHighCloudAmount(record.getByte(16))
                               .changeCode(record.getByte(17))
                               .solarAltitude(record.getShort(18))
                               .relativeLunarIlluminance(record.getByte(19))
                               .seaLevelPressure(record.getShort(20))
                               .windSpeed(record.getShort(21))
                               .windDirection(record.getShort(22))
                               .airTemperature(record.getShort(23))
                               .dewPointDepression(record.getShort(24))
                               .stationElevation(record.getShort(25))
                               .windSpeedIndicator(record.getByte(26))
                               .seaLevelPressureFlag(record.getByte(27))
                               .build();
    }

    @Override
    public JsonDeserializer<? extends ADBEntity> buildDeserializer() {
        return new CloudObservationDeserializer();
    }
}
