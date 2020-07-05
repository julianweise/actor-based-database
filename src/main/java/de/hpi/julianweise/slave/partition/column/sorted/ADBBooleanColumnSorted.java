package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityBooleanEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBBooleanColumnSorted extends ADBColumnSorted {
    private boolean[] values;

    public ADBBooleanColumnSorted(int nodeId, int partitionId, boolean[] values, int[] sorted2Original, int[] original2Sorted) {
        super(nodeId, partitionId, sorted2Original, original2Sorted);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityBooleanEntry(id, this.values[i]);
    }
}
