package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;

import java.util.List;

public interface ADBAttributeComparisonStrategy {

    List<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                             List<ADBComparable2IntPair> left,
                             List<ADBComparable2IntPair> right,
                             int estimatedResultSize);
}
