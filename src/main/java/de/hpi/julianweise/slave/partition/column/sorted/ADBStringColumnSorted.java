package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityStringEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBStringColumnSorted extends ADBColumnSorted {
    private String[] values;

    public ADBStringColumnSorted(int nodeId, int partitionId, String[] values, int[] originalIndices, int[] original2Sorted) {
        super(nodeId, partitionId, originalIndices, original2Sorted);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityStringEntry(id, this.values[i]);
    }
}
