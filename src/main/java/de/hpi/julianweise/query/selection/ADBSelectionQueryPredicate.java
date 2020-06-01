package de.hpi.julianweise.query.selection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ADBSelectionQueryPredicate implements ADBQueryTerm {

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = int.class, name = "Integer"),
                          @JsonSubTypes.Type(value = float.class, name = "Float"),
                          @JsonSubTypes.Type(value = double.class, name = "Double"),
                          @JsonSubTypes.Type(value = char.class, name = "Character"),
                          @JsonSubTypes.Type(value = boolean.class, name = "Boolean"),
                  })
    private ADBPredicateConstant value;
    private String fieldName;
    private ADBQueryTerm.RelationalOperator operator;

    @Override
    public String toString() {
        return "[SelectionPredicate] " + this.fieldName + " " + this.operator + " " + this.value;
    }
}