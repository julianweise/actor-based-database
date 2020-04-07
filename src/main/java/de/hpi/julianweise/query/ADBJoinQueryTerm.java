package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ADBJoinQueryTerm implements ADBQueryTerm {

    private static RelationalOperator reverseOperator(RelationalOperator operator) {
        if (operator == RelationalOperator.LESS) {
            return RelationalOperator.GREATER;
        }
        if (operator == RelationalOperator.GREATER) {
            return RelationalOperator.LESS;
        }
        if (operator == RelationalOperator.LESS_OR_EQUAL) {
            return RelationalOperator.GREATER_OR_EQUAL;
        }
        if (operator == RelationalOperator.GREATER_OR_EQUAL) {
            return RelationalOperator.LESS_OR_EQUAL;
        }
        return operator;
    }

    private ADBQueryTerm.RelationalOperator operator;
    private String leftHandSideAttribute;
    private String rightHandSideAttribute;

    @Override
    public String toString() {
        return "[JoinTerm] " + this.leftHandSideAttribute + " " + this.operator + " " + this.rightHandSideAttribute;
    }

    @JsonIgnore
    public ADBJoinQueryTerm getReverse() {
        return new ADBJoinQueryTerm(ADBJoinQueryTerm.reverseOperator(this.operator), this.rightHandSideAttribute,
                this.leftHandSideAttribute);
    }
}
