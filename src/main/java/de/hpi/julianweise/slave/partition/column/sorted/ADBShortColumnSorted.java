package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityShortEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBShortColumnSorted extends ADBColumnSorted {
    private short[] values;

    public ADBShortColumnSorted(int nodeId, int partitionId, short[] values, short[] originalIndices) {
        super(nodeId, partitionId, originalIndices);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityShortEntry(id, this.values[i]);
    }
}
