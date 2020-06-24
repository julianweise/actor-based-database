package de.hpi.julianweise.slave.query.join.filter;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;

public interface ADBJoinPartitionFilterStrategy {

    ADBEntityEntry getMinValueForLeft(String attributeName);
    ADBEntityEntry getMaxValueForLeft(String attributeName);
    ADBEntityEntry getMinValueForRight(String attributeName);
    ADBEntityEntry getMaxValueForRight(String attributeName);
}
