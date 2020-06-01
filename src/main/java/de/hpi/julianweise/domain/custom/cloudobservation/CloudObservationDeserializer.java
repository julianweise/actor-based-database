package de.hpi.julianweise.domain.custom.cloudobservation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class CloudObservationDeserializer extends JsonDeserializer<CloudObservation> {

    @Override
    public CloudObservation deserialize(JsonParser jsonParser, DeserializationContext d) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return CloudObservation.builder()
                               ._timestamp(node.get("timestamp").asInt())
                               .skyBrightnessIndicator(node.get("skyBrightnessIndicator").asBoolean())
                               .latitude(node.get(2).shortValue())
                               .longitude(node.get(3).shortValue())
                               .stationNumber(node.get(4).asInt())
                               .landIndicator(node.get(5).asBoolean())
                               .presentWeather(node.get(6).binaryValue()[0])
                               .totalCloudCover(node.get(7).binaryValue()[0])
                               .lowerCloudAmount(node.get(8).binaryValue()[0])
                               .lowerCloudBasedHeight(node.get(9).binaryValue()[0])
                               .lowCloudType(node.get(10).binaryValue()[0])
                               .middleCloudType(node.get(11).binaryValue()[0])
                               .highCloudType(node.get(12).binaryValue()[0])
                               .middleCloudAmount(node.get(13).shortValue())
                               .highCloudAmount(node.get(14).shortValue())
                               .nonOverheadMiddleCloudAmount(node.get(15).binaryValue()[0])
                               .nonOverheadHighCloudAmount(node.get(16).binaryValue()[0])
                               .changeCode(node.get(17).binaryValue()[0])
                               .solarAltitude(node.get(18).shortValue())
                               .relativeLunarIlluminance(node.get(19).binaryValue()[0])
                               .seaLevelPressure(node.get(20).shortValue())
                               .windSpeed(node.get(21).shortValue())
                               .windDirection(node.get(22).shortValue())
                               .airTemperature(node.get(23).shortValue())
                               .dewPointDepression(node.get(24).shortValue())
                               .stationElevation(node.get(25).shortValue())
                               .windSpeedIndicator(node.get(26).binaryValue()[0])
                               .seaLevelPressureFlag(node.get(27).binaryValue()[0])
                               .build();
    }
}
