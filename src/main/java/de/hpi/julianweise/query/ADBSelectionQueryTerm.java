package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ADBSelectionQueryTerm implements ADBQueryTerm {

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                          @JsonSubTypes.Type(value = Float.class, name = "Float"),
                          @JsonSubTypes.Type(value = Double.class, name = "Double"),
                          @JsonSubTypes.Type(value = Character.class, name = "Character"),
                          @JsonSubTypes.Type(value = Boolean.class, name = "Boolean"),
                  })
    Comparable<?> value;
    String fieldName;
    ADBQueryTerm.RelationalOperator operator;

    @Override
    public String toString() {
        return "[SelectionTerm] " + this.fieldName + " " + this.operator + " " + this.value;
    }
}