package de.hpi.julianweise.query;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Getter
@NoArgsConstructor
@SuperBuilder
public abstract class ADBQuery {

    public enum RelationalOperator {
        UNSPECIFIED,
        EQUALITY,
        INEQUALITY,
        GREATER_OR_EQUAL,
        GREATER,
        LESS_OR_EQUAL,
        LESS
    }

    @Getter
    @SuperBuilder
    @NoArgsConstructor
    public static abstract class QueryTerm {
        protected String fieldName;
        protected RelationalOperator operator;

    }
}
