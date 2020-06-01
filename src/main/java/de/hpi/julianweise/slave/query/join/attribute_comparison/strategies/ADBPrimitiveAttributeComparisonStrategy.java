package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBPrimitiveAttributeComparisonStrategy implements ADBAttributeComparisonStrategy {

    @Override
    public ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                          ObjectList<ADBEntityEntry> left,
                                          ObjectList<ADBEntityEntry> right,
                                          int estimatedResultSize) {
        ObjectArrayList<ADBKeyPair> joinCandidates = new ObjectArrayList<>(estimatedResultSize);

        for (ADBEntityEntry leftValue : left) {
            for (ADBEntityEntry rightValue : right) {
                if (ADBEntityEntry.matches(leftValue, rightValue, operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getId(), rightValue.getId()));
                }
            }
        }

        joinCandidates.trim();
        return joinCandidates;
    }

}
