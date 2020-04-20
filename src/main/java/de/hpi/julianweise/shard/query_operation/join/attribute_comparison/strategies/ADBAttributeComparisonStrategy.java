package de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

public interface ADBAttributeComparisonStrategy {

    List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                             List<ADBPair<Comparable<?>, Integer>> left,
                             List<ADBPair<Comparable<?>, Integer>> right,
                             int estimatedResultSize);
}
