package de.hpi.julianweise.domain.custom.tpch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class LineItemAndPartDeserializer extends JsonDeserializer<LineItemAndPart> {

    @Override
    public LineItemAndPart deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return LineItemAndPart
                .builder()
                .l_orderkey(node.get("l_orderkey").asInt())
                .l_partkey(node.get("l_partkey").asInt())
                .l_suppkey(node.get("l_suppkey").asInt())
                .l_linenumber(node.get("l_linenumber").asInt())
                .l_quantity(node.get("l_quantity").floatValue())
                .l_extendedprice(node.get("l_extendedprice").floatValue())
                .l_discount(node.get("l_discount").floatValue())
                .l_tax(node.get("l_tax").floatValue())
                .l_returnflag(node.get("l_returnflag").asText().charAt(0))
                .l_linestatus(node.get("l_linestatus").asText().charAt(0))
                .l_shipdate(node.get("l_shipdate").asText())
                .l_commitdate(node.get("l_commitdate").asText())
                .l_receiptdate(node.get("l_receiptdate").asText())
                .l_shipinstruct(node.get("l_shipinstruct").asText())
                .l_shipmode(node.get("l_shipmode").asText())
                .l_comment(node.get("l_comment").asText())
                .p_partkey(node.get("p_partkey").asInt())
                .p_name(node.get("p_name").asText())
                .p_mfgr(node.get("p_mfgr").asText())
                .p_brand(node.get("p_brand").asText())
                .p_type(node.get("p_type").asText())
                .p_size(node.get("p_size").asInt())
                .p_container(node.get("p_container").asText())
                .p_retailprice(node.get("p_retailprice").floatValue())
                .p_comment(node.get("p_comment").asText())
                .build();
    }
}
