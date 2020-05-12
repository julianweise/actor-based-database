package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
public class ADBPartitionHeader {

    private int id;
    private Map<String, Comparable<Object>> minValues;
    private Map<String, Comparable<Object>> maxValues;
    private transient List<ADBEntity> data;

    public ADBPartitionHeader(Map<String, Comparable<Object>> minValues, Map<String, Comparable<Object>> maxValues,
                              List<ADBEntity> data, int id) {
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.data = data;
        this.id = id;
    }

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

    public boolean isRelevant(ADBSelectionQuery selectionQuery) {
        return selectionQuery.getTerms().stream().allMatch(this::isRelevant);
    }

    public boolean isRelevant(ADBSelectionQueryTerm selectionQueryTerm) {
        Comparable<Object> min = this.minValues.get(selectionQueryTerm.getFieldName());
        Comparable<Object> max = this.maxValues.get(selectionQueryTerm.getFieldName());

        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS)) {
            return min.compareTo(selectionQueryTerm.getValue()) < 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)) {
            return min.compareTo(selectionQueryTerm.getValue()) <= 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER)) {
            return max.compareTo(selectionQueryTerm.getValue()) > 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)) {
            return max.compareTo(selectionQueryTerm.getValue()) > 0;
        }
        return min.compareTo(selectionQueryTerm.getValue()) <= 0 && max.compareTo(selectionQueryTerm.getValue()) >= 0;
    }
}
