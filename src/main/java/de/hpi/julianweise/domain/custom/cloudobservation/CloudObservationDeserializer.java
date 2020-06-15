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
                               ._timestamp(node.get("_timestamp").asInt())
                               .skyBrightnessIndicator(node.get("skyBrightnessIndicator").asBoolean())
                               .latitude(node.get("latitude").shortValue())
                               .longitude(node.get("longitude").intValue())
                               .stationNumber(node.get("stationNumber").asInt())
                               .landIndicator(node.get("landIndicator").asBoolean())
                               .presentWeather((byte) node.get("presentWeather").asInt())
                               .totalCloudCover((byte) node.get("totalCloudCover").asInt())
                               .lowerCloudAmount((byte) node.get("lowerCloudAmount").asInt())
                               .lowerCloudBasedHeight((byte) node.get("lowerCloudBasedHeight").asInt())
                               .lowCloudType((byte) node.get("lowCloudType").asInt())
                               .middleCloudType((byte) node.get("middleCloudType").asInt())
                               .highCloudType((byte) node.get("highCloudType").asInt())
                               .middleCloudAmount(node.get("middleCloudAmount").shortValue())
                               .highCloudAmount(node.get("highCloudAmount").shortValue())
                               .nonOverheadMiddleCloudAmount((byte) node.get("nonOverheadMiddleCloudAmount").asInt())
                               .nonOverheadHighCloudAmount((byte) node.get("nonOverheadHighCloudAmount").asInt())
                               .changeCode((byte) node.get("changeCode").asInt())
                               .solarAltitude(node.get("solarAltitude").shortValue())
                               .relativeLunarIlluminance((byte) node.get("relativeLunarIlluminance").asInt())
                               .seaLevelPressure(node.get("seaLevelPressure").shortValue())
                               .windSpeed(node.get("windSpeed").shortValue())
                               .windDirection(node.get("windDirection").shortValue())
                               .airTemperature(node.get("airTemperature").shortValue())
                               .dewPointDepression(node.get("dewPointDepression").shortValue())
                               .stationElevation(node.get("stationElevation").shortValue())
                               .windSpeedIndicator((byte) node.get("windSpeedIndicator").asInt())
                               .seaLevelPressureFlag((byte) node.get("seaLevelPressureFlag").asInt())
                               .build();
    }
}
