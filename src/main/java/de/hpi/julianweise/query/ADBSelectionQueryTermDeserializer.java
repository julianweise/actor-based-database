package de.hpi.julianweise.query;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Type;

@AllArgsConstructor
public class ADBSelectionQueryTermDeserializer extends JsonDeserializer<ADBSelectionQueryTerm> {

    private final Class<? extends ADBEntityType> entityClass;

    @SneakyThrows
    @Override
    public ADBSelectionQueryTerm deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return ADBSelectionQueryTerm.builder()
                                                   .fieldName(node.get("fieldName").asText())
                                                   .operator(ADBQueryTerm.RelationalOperator.valueOf(node.get("operator").asText()))
                                                   .value(this.convertToCorrectDataFormat(node.get("fieldName").asText(), node))
                                                   .build();
    }

    private Comparable<?> convertToCorrectDataFormat(String fieldName, JsonNode node) throws NoSuchFieldException {
        Type valueType = this.entityClass.getDeclaredField(fieldName).getType();
        if (valueType.equals(Integer.class) || valueType.equals(int.class)) {
            return node.get("value").asInt();
        } else if (valueType.equals(Float.class) || valueType.equals(float.class)) {
            return node.get("value").asDouble();
        } else if (valueType.equals(Double.class) || valueType.equals(double.class)) {
            return node.get("value").asDouble();
        } else if (valueType.equals(String.class)) {
            return node.get("value").asText();
        } else if (valueType.equals(Boolean.class) || valueType.equals(boolean.class)) {
            return node.get("value").asBoolean();
        } else if (valueType.equals(Character.class) || valueType.equals(char.class)) {
            return node.get("value").asText().charAt(0);
        }
        return node.get("value").asText();
    }
}
