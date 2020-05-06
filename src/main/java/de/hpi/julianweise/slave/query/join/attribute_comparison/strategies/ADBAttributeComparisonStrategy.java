package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

public interface ADBAttributeComparisonStrategy {

    List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                             List<ADBPair<Comparable<Object>, Integer>> left,
                             List<ADBPair<Comparable<Object>, Integer>> right,
                             int estimatedResultSize);
}
