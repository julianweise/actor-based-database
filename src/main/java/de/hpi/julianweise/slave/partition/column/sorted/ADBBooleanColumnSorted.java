package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityBooleanEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBBooleanColumnSorted extends ADBColumnSorted {
    private boolean[] values;

    public ADBBooleanColumnSorted(int nodeId, int partitionId, boolean[] values, int[] originalIndices) {
        super(nodeId, partitionId, originalIndices);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityBooleanEntry(id, this.values[i]);
    }
}
