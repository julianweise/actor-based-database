package de.hpi.julianweise.slave.query.join.filter;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ADBNoFilterStrategy implements ADBJoinPartitionFilterStrategy {

    ADBPartitionJoinTask joinTask;

    @Override
    public ADBEntityEntry getMinValueForLeft(String attributeName) {
        return this.joinTask.getLeftHeader().getMinValues().get(attributeName);
    }

    @Override
    public ADBEntityEntry getMaxValueForLeft(String attributeName) {
        return this.joinTask.getLeftHeader().getMaxValues().get(attributeName);
    }

    @Override
    public ADBEntityEntry getMinValueForRight(String attributeName) {
        return this.joinTask.getRightHeader().getMinValues().get(attributeName);
    }

    @Override public ADBEntityEntry getMaxValueForRight(String attributeName) {
        return this.joinTask.getRightHeader().getMaxValues().get(attributeName);
    }
}
