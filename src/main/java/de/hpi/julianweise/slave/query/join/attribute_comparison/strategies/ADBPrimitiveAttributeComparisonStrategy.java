package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;

import java.util.ArrayList;
import java.util.List;

public class ADBPrimitiveAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                    List<ADBComparable2IntPair> left,
                                    List<ADBComparable2IntPair> right,
                                    int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinCandidates = new ArrayList<>(estimatedResultSize);

        for (ADBComparable2IntPair leftValue : left) {
            for (ADBComparable2IntPair rightValue : right) {
                if (ADBEntity.matches(leftValue.getKey(), rightValue.getKey(), operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getValue(), rightValue.getValue()));
                }
            }
        }

        joinCandidates.trimToSize();
        return joinCandidates;
    }

}
