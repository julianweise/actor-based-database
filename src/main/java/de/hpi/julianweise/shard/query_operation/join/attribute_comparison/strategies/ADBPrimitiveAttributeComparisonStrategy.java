package de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.ArrayList;
import java.util.List;

public class ADBPrimitiveAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    @SuppressWarnings("unchecked")
    public List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                    List<ADBPair<Comparable<?>, Integer>> left,
                                    List<ADBPair<Comparable<?>, Integer>> right,
                                    int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinCandidates = new ArrayList<>(estimatedResultSize);

        for (ADBPair<Comparable<?>, Integer> leftValue : left) {
            for (ADBPair<Comparable<?>, Integer> rightValue : right) {
                if (ADBEntityType.matches((Comparable<Object>) leftValue.getKey(), rightValue.getKey(), operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getValue(), rightValue.getValue()));
                }
            }
        }

        joinCandidates.trimToSize();
        return joinCandidates;
    }

}
