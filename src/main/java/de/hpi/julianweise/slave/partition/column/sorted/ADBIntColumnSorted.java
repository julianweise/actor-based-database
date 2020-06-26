package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBIntColumnSorted extends ADBColumnSorted {
    private int[] values;

    public ADBIntColumnSorted(int nodeId, int partitionId, int[] values, short[] originalIndices) {
        super(nodeId, partitionId, originalIndices);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityIntEntry(id, this.values[i]);
    }
}
