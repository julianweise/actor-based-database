package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.ArrayList;
import java.util.List;

public class ADBPrimitiveAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                    List<ADBPair<Comparable<Object>, Integer>> left,
                                    List<ADBPair<Comparable<Object>, Integer>> right,
                                    int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinCandidates = new ArrayList<>(estimatedResultSize);

        for (ADBPair<Comparable<Object>, Integer> leftValue : left) {
            for (ADBPair<Comparable<Object>, Integer> rightValue : right) {
                if (ADBEntity.matches(leftValue.getKey(), rightValue.getKey(), operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getValue(), rightValue.getValue()));
                }
            }
        }

        joinCandidates.trimToSize();
        return joinCandidates;
    }

}
