package de.hpi.julianweise.domain.custom.flight;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class FlightDeserializer extends JsonDeserializer<FLight> {

    @Override
    public FLight deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);


        return FLight.builder()
                     .FL_DATA(node.get("FL_DATA").asInt())
                     .OP_UNIQUE_CARRIER(node.get("OP_UNIQUE_CARRIER").asText())
                     .ORIGIN_AIRPORT_ID(node.get("ORIGIN_AIRPORT_ID").shortValue())
                     .DEST_AIRPORT_ID(node.get("DEST_AIRPORT_ID").shortValue())
                     .CRS_DEP_TIME(node.get("CRS_DEP_TIME").asText())
                     .DEP_TIME(node.get("DEP_TIME").asText())
                     .DEP_DELAY(node.get("CRS_DEP_TIME").floatValue())
                     .WHEELS_ON(node.get("WHEELS_ON").asText())
                     .TAXI_IN(node.get("TAXI_IN").floatValue())
                     .CRS_ARR_TIME(node.get("CRS_ARR_TIME").asText())
                     .ARR_TIME(node.get("ARR_TIME").asText())
                     .ARR_DELAY(node.get("ARR_DELAY").floatValue())
                     .CRS_ELAPSED_TIME(node.get("CRS_ELAPSED_TIME").floatValue())
                     .ACTUAL_ELAPSED_TIME(node.get("ACTUAL_ELAPSED_TIME").floatValue())
                     .DISTANCE(node.get("DISTANCE").floatValue())
                     .build();
    }
}
