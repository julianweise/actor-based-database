package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityByteEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBByteColumnSorted extends ADBColumnSorted {
    private byte[] values;

    public ADBByteColumnSorted(int nodeId, int partitionId, byte[] values, int[] sorted2Original, int[] original2Sorted) {
        super(nodeId, partitionId, sorted2Original, original2Sorted);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityByteEntry(id, this.values[i]);
    }
}
