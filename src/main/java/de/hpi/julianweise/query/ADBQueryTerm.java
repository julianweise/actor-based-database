package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;

@JsonSubTypes({
                      @JsonSubTypes.Type(value = ADBSelectionQuery.class, name = "ADBSelectionQueryTerm"),
                      @JsonSubTypes.Type(value = ADBJoinQueryPredicate.class, name = "ADBJoinQueryTerm"),
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
