package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

@Getter
@NoArgsConstructor
public class ADBPartitionHeader {

    private int id;
    private Map<String, ADBEntityEntry> minValues;
    private Map<String, ADBEntityEntry> maxValues;

    public ADBPartitionHeader(Map<String, ADBEntityEntry> minValues, Map<String, ADBEntityEntry> maxValues, int id) {
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.id = id;
    }

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQuery joinQuery) {
        return joinQuery.getPredicates().stream().allMatch(predicate -> this.isOverlapping(b, predicate));
    }

    public boolean isOverlapping(ADBPartitionHeader b, ADBJoinQueryPredicate joinPredicate) {
        ADBEntityEntry leftMin = this.minValues.get(joinPredicate.getLeftHandSideAttribute());
        ADBEntityEntry leftMax = this.maxValues.get(joinPredicate.getLeftHandSideAttribute());
        ADBEntityEntry rightMin = b.minValues.get(joinPredicate.getRightHandSideAttribute());
        ADBEntityEntry rightMax = b.maxValues.get(joinPredicate.getRightHandSideAttribute());
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS)) {
            return !(ADBComparator.getFor(leftMin.getValueField(), rightMax.getValueField()).compare(leftMin, rightMax) >= 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)) {
            return !(ADBComparator.getFor(leftMin.getValueField(), rightMax.getValueField()).compare(leftMin, rightMax) > 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER)) {
            return !(ADBComparator.getFor(leftMax.getValueField(), rightMin.getValueField()).compare(leftMax, rightMin) <= 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)) {
            return !(ADBComparator.getFor(leftMax.getValueField(), rightMin.getValueField()).compare(leftMax, rightMin) < 0);
        }
        if (joinPredicate.getOperator().equals(ADBQueryTerm.RelationalOperator.EQUALITY)) {
            ADBComparator x = ADBComparator.getFor(leftMax.getValueField(), rightMin.getValueField());
            ADBComparator y = ADBComparator.getFor(leftMin.getValueField(), rightMax.getValueField());
            return x.compare(leftMax, rightMin) >= 0 && y.compare(leftMin, rightMax) <= 0;
        }
        return true;
    }

    public boolean isRelevant(ADBSelectionQuery selectionQuery) {
        return selectionQuery.getPredicates().stream().allMatch(this::isRelevant);
    }

    public boolean isRelevant(ADBSelectionQueryPredicate selectionQueryTerm) {
        ADBEntityEntry min = this.minValues.get(selectionQueryTerm.getFieldName());
        ADBEntityEntry max = this.maxValues.get(selectionQueryTerm.getFieldName());

        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS)) {
            ADBPredicateConstant value = selectionQueryTerm.getValue();
            return ADBComparator.getFor(min.getValueField(), value.getValueField()).compare(min, value) < 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)) {
            ADBPredicateConstant value = selectionQueryTerm.getValue();
            return ADBComparator.getFor(min.getValueField(), value.getValueField()).compare(min, value) <= 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER)) {
            ADBPredicateConstant value = selectionQueryTerm.getValue();
            return ADBComparator.getFor(max.getValueField(), value.getValueField()).compare(max, value) > 0;
        }
        if (selectionQueryTerm.getOperator().equals(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)) {
            ADBPredicateConstant value = selectionQueryTerm.getValue();
            return ADBComparator.getFor(max.getValueField(), value.getValueField()).compare(max, value) >= 0;
        }
        ADBPredicateConstant value = selectionQueryTerm.getValue();
        ADBComparator a = ADBComparator.getFor(min.getValueField(), value.getValueField());
        ADBComparator b = ADBComparator.getFor(max.getValueField(), value.getValueField());
        return a.compare(min, value) <= 0 && b.compare(max, value) >= 0;
    }
}
