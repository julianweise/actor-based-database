package de.hpi.julianweise.domain.custom.cloudobservation;

import com.fasterxml.jackson.databind.JsonDeserializer;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;
import org.apache.commons.csv.CSVRecord;

import static java.lang.Integer.parseInt;

public class CloudObservationFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return CloudObservation.class;
    }

    @Override
    public ADBEntity build(CSVRecord record) {
        return CloudObservation.builder()
                               ._timestamp(parseInt(record.get(0), 10))
                               .skyBrightnessIndicator(record.get(1).equals("1"))
                               .latitude(Short.parseShort(record.get(2)))
                               .longitude(Short.parseShort(record.get(3)))
                               .stationNumber(Integer.parseInt(record.get(4), 10))
                               .landIndicator(record.get(5).equals("1"))
                               .presentWeather(Byte.parseByte(record.get(6)))
                               .totalCloudCover(Byte.parseByte(record.get(7)))
                               .lowerCloudAmount(Byte.parseByte(record.get(8)))
                               .lowerCloudBasedHeight(Byte.parseByte(record.get(9)))
                               .lowCloudType(Byte.parseByte(record.get(10)))
                               .middleCloudType(Byte.parseByte(record.get(11)))
                               .highCloudType(Byte.parseByte(record.get(12)))
                               .middleCloudAmount(Short.parseShort(record.get(13)))
                               .highCloudAmount(Short.parseShort(record.get(14)))
                               .nonOverheadMiddleCloudAmount(Byte.parseByte(record.get(15)))
                               .nonOverheadHighCloudAmount(Byte.parseByte(record.get(16)))
                               .changeCode(Byte.parseByte(record.get(17)))
                               .solarAltitude(Short.parseShort(record.get(18)))
                               .relativeLunarIlluminance(Byte.parseByte(record.get(19)))
                               .seaLevelPressure(Short.parseShort(record.get(20)))
                               .windSpeed(Short.parseShort(record.get(21)))
                               .windDirection(Short.parseShort(record.get(22)))
                               .airTemperature(Short.parseShort(record.get(23)))
                               .dewPointDepression(Short.parseShort(record.get(24)))
                               .stationElevation(Short.parseShort(record.get(25)))
                               .windSpeedIndicator(Byte.parseByte(record.get(26)))
                               .seaLevelPressureFlag(Byte.parseByte(record.get(27)))
                               .build();
    }

    @Override
    public JsonDeserializer<? extends ADBEntity> buildDeserializer() {
        return null;
    }
}
