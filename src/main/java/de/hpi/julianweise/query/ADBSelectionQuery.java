package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

public class ADBSelectionQuery extends ADBQuery {

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class SelectionQueryTerm extends QueryTerm {
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

        @Override
        public String toString() {
            return "[Term] " + this.fieldName + " " + this.operator + " " + this.value;
        }
    }

    @Getter
    private final List<SelectionQueryTerm> terms = new ArrayList<>();

    public void addTerm(SelectionQueryTerm term) {
        this.terms.add(term);
    }

    @Override
    public String toString() {
        return "[Query] " + this.terms.stream()
                                      .map(SelectionQueryTerm::toString)
                                      .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
