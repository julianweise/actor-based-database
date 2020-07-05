package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityFloatEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBFloatColumnSorted extends ADBColumnSorted {
    private float[] values;

    public ADBFloatColumnSorted(int nodeId, int partitionId, float[] values, int[] sorted2Original, int[] original2Sorted) {
        super(nodeId, partitionId, sorted2Original, original2Sorted);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityFloatEntry(id, this.values[i]);
    }
}
