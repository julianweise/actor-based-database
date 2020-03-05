package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class ADBSelectionQuery extends ADBQuery {

    public enum RelationalOperator {
        UNSPECIFIED,
        EQUALITY,
        INEQUALITY,
        GREATER_OR_EQUAL,
        GREATER,
        LESS_OR_EQUAL,
        LESS
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class QueryTerm {
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
        RelationalOperator operator;

        @Override
        public String toString() {
            return "[Term] " + this.fieldName + " " + this.operator + " " + this.value;
        }

    }

    @Getter
    private final List<QueryTerm> terms = new ArrayList<>();

    public void addTerm(QueryTerm term) {
        this.terms.add(term);
    }

    @Override
    public String toString() {
        return "[Query] " + this.terms.stream()
                                      .map(QueryTerm::toString)
                                      .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
