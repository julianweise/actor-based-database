package de.hpi.julianweise.slave.query.join.attribute_comparison.strategies;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectList;

public interface ADBAttributeComparisonStrategy {

    ObjectList<ADBKeyPair> compare(ADBQueryTerm.RelationalOperator operator,
                                   ObjectList<ADBEntityEntry> left,
                                   ObjectList<ADBEntityEntry> right,
                                   int estimatedResultSize);
}
