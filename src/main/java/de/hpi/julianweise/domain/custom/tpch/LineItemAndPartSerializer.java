package de.hpi.julianweise.domain.custom.tpch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class LineItemAndPartSerializer extends JsonDeserializer<LineItemAndPart> {

    @Override
    public LineItemAndPart deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return LineItemAndPart
                .builder()
                .l_orderkey(node.get("").asInt())
                .l_partkey(node.get("").asInt())
                .l_suppkey(node.get("").asInt())
                .l_linenumber(node.get("").asInt())
                .l_quantity(node.get("").floatValue())
                .l_extendedprice(node.get("").floatValue())
                .l_discount(node.get("").floatValue())
                .l_tax(node.get("").floatValue())
                .l_returnflag(node.get("").asText().charAt(0))
                .l_linestatus(node.get("").asText().charAt(0))
                .l_shipdate(node.get("").asText())
                .l_commitdate(node.get("").asText())
                .l_receiptdate(node.get("").asText())
                .l_shipinstruct(node.get("").asText())
                .l_shipmode(node.get("").asText())
                .l_comment(node.get("").asText())
                .p_partkey(node.get("").asInt())
                .p_name(node.get("").asText())
                .p_mfgr(node.get("").asText())
                .p_brand(node.get("").asText())
                .p_type(node.get("").asText())
                .p_size(node.get("").asInt())
                .p_container(node.get("").asText())
                .p_retailprice(node.get("").floatValue())
                .p_comment(node.get("").asText())
                .build();
    }
}
