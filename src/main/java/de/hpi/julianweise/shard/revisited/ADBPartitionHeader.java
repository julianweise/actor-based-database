package de.hpi.julianweise.shard.revisited;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class ADBPartitionHeader {

    private final Map<String, Comparable<Object>> minValues;
    private final Map<String, Comparable<Object>> maxValues;
    private transient final List<ADBEntity> data;

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQuery joinQuery) {
        return joinQuery.getTerms().stream().allMatch(term -> this.isOverlapping(b, term));
    }

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQueryTerm joinQueryTerm) {
        Comparable<Object> leftMin = this.minValues.get(joinQueryTerm.getLeftHandSideAttribute());
        Comparable<Object> leftMax = this.maxValues.get(joinQueryTerm.getLeftHandSideAttribute());
        Comparable<Object> rightMin = b.minValues.get(joinQueryTerm.getRightHandSideAttribute());
        Comparable<Object> rightMax = b.maxValues.get(joinQueryTerm.getRightHandSideAttribute());
        if (joinQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS)) {
            return leftMin.compareTo(rightMax) < 0;
        }
        if (joinQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)) {
            return leftMin.compareTo(rightMax) <= 0;
        }
        if (joinQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER)) {
            return leftMax.compareTo(rightMin) > 0;
        }
        if (joinQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)) {
            return leftMax.compareTo(rightMin) >= 0;
        }
        return leftMax.compareTo(rightMin) >= 0 || leftMin.compareTo(rightMax) > 0;
    }
}
