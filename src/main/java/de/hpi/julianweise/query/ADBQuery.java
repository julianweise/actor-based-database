package de.hpi.julianweise.query;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
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
}
