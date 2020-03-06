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
    String fieldName;
    String targetFieldName;

    @Override
    public String toString() {
        return "[JoinTerm] " + this.fieldName + " " + this.operator + " " + this.targetFieldName;
    }
}
