package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.query.join.ADBOffsetCalculator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

@SuppressWarnings("unused")
public class ADBOffsetAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                          ObjectList<ADBComparable2IntPair> left,
                                          ObjectList<ADBComparable2IntPair> right,
                                          int estimatedResultSize) {
        int[] offset = ADBOffsetCalculator.calc(left, right);
        return this.performComparison(operator, left, right, offset, estimatedResultSize);
    }

    private ObjectArrayList<ADBKeyPair> performComparison(ADBQueryTerm.RelationalOperator operator,
                                                    ObjectList<ADBComparable2IntPair> left,
                                                    ObjectList<ADBComparable2IntPair> right,
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

    private ObjectArrayList<ADBKeyPair> compareGreater(ObjectList<ADBComparable2IntPair> left,
                                                 ObjectList<ADBComparable2IntPair> right,
                                                 int[] offset,
                                                 int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int corr = left.get(a).getKey().compareTo(right.get(offset[a]).getKey()) < 0 ? -1 : 0;
            for (int b = offset[a] + corr; b > -1; b--) {
                if (left.get(a).getKey().equals(right.get(b).getKey())) {
                    continue;
                }
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareGreaterEquals(ObjectList<ADBComparable2IntPair> left,
                                                       ObjectList<ADBComparable2IntPair> right,
                                                       int[] offset,
                                                       int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection = left.get(a).getKey().compareTo(right.get(offset[a]).getKey()) < 0 ? -1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1; b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareLess(ObjectList<ADBComparable2IntPair> left,
                                              ObjectList<ADBComparable2IntPair> right,
                                              int[] offset,
                                              int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection = left.get(a).getKey().compareTo(right.get(offset[a]).getKey()) > -1 ? 1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareLessEqual(ObjectList<ADBComparable2IntPair> left,
                                                   ObjectList<ADBComparable2IntPair> right,
                                                   int[] offset,
                                                   int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection = 0;
            while (offset[a] - offsetCorrection - 1 > 0 && left.get(a).getKey().equals(right.get(offset[a] - offsetCorrection - 1).getKey())) offsetCorrection--;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ObjectArrayList<ADBKeyPair> compareEqual(ObjectList<ADBComparable2IntPair> left,
                                               ObjectList<ADBComparable2IntPair> right,
                                               int[] offset,
                                               int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinTuples = new ObjectArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            for (int b = offset[a]; b > -1 && left.get(a).getKey().equals(right.get(b).getKey()); b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

}
