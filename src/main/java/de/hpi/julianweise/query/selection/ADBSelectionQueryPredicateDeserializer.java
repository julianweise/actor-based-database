package de.hpi.julianweise.query.selection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.selection.constant.ADBPredicateBooleanConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateByteConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateDoubleConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateFloatConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateStringConstant;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class ADBSelectionQueryPredicateDeserializer extends JsonDeserializer<ADBSelectionQueryPredicate> {

    private final Class<? extends ADBEntity> entityClass;

    @SneakyThrows
    @Override
    public ADBSelectionQueryPredicate deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return ADBSelectionQueryPredicate.builder()
                                         .fieldName(node.get("fieldName").asText())
                                         .operator(ADBQueryTerm.RelationalOperator.valueOf(node.get("operator").asText()))
                                         .value(this.convertToCorrectDataFormat(node.get("fieldName").asText(), node))
                                         .build();
    }

    @SneakyThrows
    private ADBPredicateConstant convertToCorrectDataFormat(String fieldName, JsonNode node) {
        Class<?> valueType = this.entityClass.getDeclaredField(fieldName).getType();
        if (valueType.equals(Integer.class) || valueType.equals(int.class)) {
            return new ADBPredicateIntConstant(node.get("value").asInt());
        } else if (valueType.equals(Float.class) || valueType.equals(float.class)) {
            return new ADBPredicateFloatConstant(node.get("value").floatValue());
        } else if (valueType.equals(Double.class) || valueType.equals(double.class)) {
            return new ADBPredicateDoubleConstant(node.get("value").asDouble());
        } else if (valueType.equals(String.class)) {
            return new ADBPredicateStringConstant(node.get("value").asText());
        } else if (valueType.equals(Boolean.class) || valueType.equals(boolean.class)) {
            return new ADBPredicateBooleanConstant(node.get("value").asBoolean());
        } else if (valueType.equals(Byte.class) || valueType.equals(byte.class)) {
            return new ADBPredicateByteConstant(node.get("value").binaryValue()[0]);
        }
        return new ADBPredicateStringConstant(node.get("value").asText());
    }
}
