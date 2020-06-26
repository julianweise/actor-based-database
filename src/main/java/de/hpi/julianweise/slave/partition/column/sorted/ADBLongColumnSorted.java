package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityLongEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBLongColumnSorted extends ADBColumnSorted {
    private long[] values;

    public ADBLongColumnSorted(int nodeId, int partitionId, long[] values, short[] originalIndices) {
        super(nodeId, partitionId, originalIndices);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityLongEntry(id, this.values[i]);
    }
}
