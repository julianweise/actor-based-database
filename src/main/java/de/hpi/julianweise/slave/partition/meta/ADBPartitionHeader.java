package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryPredicate;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryPredicate;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

@Getter
@NoArgsConstructor
public class ADBPartitionHeader {

    private int id;
    private Map<String, Comparable<Object>> minValues;
    private Map<String, Comparable<Object>> maxValues;
    private transient ObjectList<ADBEntity> data;

    public ADBPartitionHeader(Map<String, Comparable<Object>> minValues, Map<String, Comparable<Object>> maxValues,
                              ObjectList<ADBEntity> data, int id) {
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.data = data;
        this.id = id;
    }

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQuery joinQuery) {
        return joinQuery.getPredicates().stream().allMatch(predicate -> this.isOverlapping(b, predicate));
    }

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQueryPredicate joinPredicate) {
        Comparable<Object> leftMin = this.minValues.get(joinPredicate.getLeftHandSideAttribute());
        Comparable<Object> leftMax = this.maxValues.get(joinPredicate.getLeftHandSideAttribute());
        Comparable<Object> rightMin = b.minValues.get(joinPredicate.getRightHandSideAttribute());
        Comparable<Object> rightMax = b.maxValues.get(joinPredicate.getRightHandSideAttribute());
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS)) {
            return !(leftMin.compareTo(rightMax) >= 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)) {
            return !(leftMin.compareTo(rightMax) > 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER)) {
            return !(leftMax.compareTo(rightMin) <= 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)) {
            return !(leftMax.compareTo(rightMin) < 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.EQUALITY)) {
            return leftMax.compareTo(rightMin) >= 0 && leftMin.compareTo(rightMax) <= 0;
        }
        return true;
    }

    public boolean isRelevant(ADBSelectionQuery selectionQuery) {
        return selectionQuery.getPredicates().stream().allMatch(this::isRelevant);
    }

    public boolean isRelevant(ADBSelectionQueryPredicate selectionQueryTerm) {
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
