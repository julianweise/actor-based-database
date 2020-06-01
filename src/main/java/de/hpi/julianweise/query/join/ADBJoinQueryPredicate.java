package de.hpi.julianweise.query.join;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.hpi.julianweise.query.ADBQueryTerm;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ADBJoinQueryPredicate implements ADBQueryTerm {

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
    public ADBJoinQueryPredicate getReverse() {
        return new ADBJoinQueryPredicate(ADBJoinQueryPredicate.reverseOperator(operator),
                this.rightHandSideAttribute, this.leftHandSideAttribute);
    }
}
