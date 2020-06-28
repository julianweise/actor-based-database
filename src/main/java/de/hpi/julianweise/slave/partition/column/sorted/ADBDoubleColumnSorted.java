package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityDoubleEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBDoubleColumnSorted extends ADBColumnSorted {
    private double[] values;

    public ADBDoubleColumnSorted(int nodeId, int partitionId, double[] values, int[] originalIndices) {
        super(nodeId, partitionId, originalIndices);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityDoubleEntry(id, this.values[i]);
    }
}
