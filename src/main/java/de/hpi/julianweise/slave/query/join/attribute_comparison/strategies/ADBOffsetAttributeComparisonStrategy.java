package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.query.join.ADBOffsetCalculator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.val;

@SuppressWarnings("unused")
public class ADBOffsetAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                          ObjectList<ADBEntityEntry> left,
                                          ObjectList<ADBEntityEntry> right,
                                          int estimatedResultSize) {
        int[] offset = ADBOffsetCalculator.calc(left, right);
        return this.performComparison(operator, left, right, offset, estimatedResultSize);
    }

    private ObjectArrayList<ADBKeyPair> performComparison(ADBQueryTerm.RelationalOperator operator,
                                                    ObjectList<ADBEntityEntry> left,
                                                    ObjectList<ADBEntityEntry> right,
                                                    int[] offset, int resultSize) {
        switch (operator) {
            case GREATER: return this.compareGreater(left, right, offset, resultSize);
            case GREATER_OR_EQUAL: return this.compareGreaterEquals(left, right, offset, resultSize);
            case LESS: return this.compareLess(left, right, offset, resultSize);
            case LESS_OR_EQUAL: return this.compareLessEqual(left, right, offset, resultSize);
            case EQUALITY: return this.compareEqual(left, right, offset, resultSize);
            default: throw new IllegalArgumentException("Operator " + operator + " is not supported." );
        }
    }

    private ObjectArrayList<ADBKeyPair> compareGreater(ObjectList<ADBEntityEntry> left,
                                                 ObjectList<ADBEntityEntry> right,
                                                 int[] offset,
                                                 int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            val comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(offset[a]).getValueField());
            int corr = comparator.compare(left.get(a), right.get(offset[a])) < 0 ? -1 : 0;
            for (int b = offset[a] + corr; b > -1; b--) {
                if (comparator.compare(left.get(a), right.get(b)) == 0 ) {
                    continue;
                }
                joinTuples.add(new ADBKeyPair(left.get(a).getId(), right.get(b).getId()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareGreaterEquals(ObjectList<ADBEntityEntry> left,
                                                       ObjectList<ADBEntityEntry> right,
                                                       int[] offset,
                                                       int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            val comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(offset[a]).getValueField());
            int offsetCorrection = comparator.compare(left.get(a), right.get(offset[a])) < 0 ? -1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1; b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getId(), right.get(b).getId()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareLess(ObjectList<ADBEntityEntry> left,
                                              ObjectList<ADBEntityEntry> right,
                                              int[] offset,
                                              int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            val comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(offset[a]).getValueField());
            int offsetCorrection = comparator.compare(left.get(a), right.get(offset[a])) > -1 ? 1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getId(), right.get(b).getId()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareLessEqual(ObjectList<ADBEntityEntry> left,
                                                   ObjectList<ADBEntityEntry> right,
                                                   int[] offset,
                                                   int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            val comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(offset[a]).getValueField());
            int offsetCorrection = 0;
            while (offset[a] - offsetCorrection - 1 > 0
                    && comparator.compare(left.get(a), right.get(offset[a] - offsetCorrection - 1)) == 0) offsetCorrection--;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getId(), right.get(b).getId()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareEqual(ObjectList<ADBEntityEntry> left,
                                               ObjectList<ADBEntityEntry> right,
                                               int[] offset,
                                               int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            val comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(offset[a]).getValueField());
            for (int b = offset[a]; b > -1 && comparator.compare(left.get(a), right.get(b)) == 0; b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getId(), right.get(b).getId()));
            }
        }
        return joinTuples;
    }

}
