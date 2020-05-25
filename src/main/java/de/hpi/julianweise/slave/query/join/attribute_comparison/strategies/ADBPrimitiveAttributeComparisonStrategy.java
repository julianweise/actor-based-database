package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBPrimitiveAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                          ObjectList<ADBComparable2IntPair> left,
                                          ObjectList<ADBComparable2IntPair> right,
                                          int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinCandidates = new ObjectArrayList<>(estimatedResultSize);

        for (ADBComparable2IntPair leftValue : left) {
            for (ADBComparable2IntPair rightValue : right) {
                if (ADBEntity.matches(leftValue.getKey(), rightValue.getKey(), operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getValue(), rightValue.getValue()));
                }
            }
        }

        joinCandidates.trim();
        return joinCandidates;
    }

}
