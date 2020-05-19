package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectList;

public interface ADBAttributeComparisonStrategy {

    ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                   ObjectList<ADBComparable2IntPair> left,
                                   ObjectList<ADBComparable2IntPair> right,
                                   int estimatedResultSize);
}
