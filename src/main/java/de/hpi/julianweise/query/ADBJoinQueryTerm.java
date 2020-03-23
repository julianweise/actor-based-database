package de.hpi.julianweise.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ADBJoinQueryTerm implements ADBQueryTerm {

    ADBQueryTerm.RelationalOperator operator;
    String sourceAttributeName;
    String targetAttributeName;

    @Override
    public String toString() {
        return "[JoinTerm] " + this.sourceAttributeName + " " + this.operator + " " + this.targetAttributeName;
    }
}
