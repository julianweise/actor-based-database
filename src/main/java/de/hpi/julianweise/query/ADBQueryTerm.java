package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;

@JsonSubTypes({
                      @JsonSubTypes.Type(value = ADBSelectionQuery.class, name = "ADBSelectionQueryTerm"),
                      @JsonSubTypes.Type(value = ADBJoinQueryTerm.class, name = "ADBJoinQueryTerm"),
              })
public interface ADBQueryTerm {

    enum RelationalOperator {
        UNSPECIFIED,
        EQUALITY,
        INEQUALITY,
        GREATER_OR_EQUAL,
        GREATER,
        LESS_OR_EQUAL,
        LESS
    }

    String toString();
}
