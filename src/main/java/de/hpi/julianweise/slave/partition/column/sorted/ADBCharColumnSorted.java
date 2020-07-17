package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityCharEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ADBCharColumnSorted extends ADBColumnSorted {
    private char[] values;

    public ADBCharColumnSorted(int nodeId, int partitionId, char[] values, int[] sorted2Original,
                               int[] original2Sorted) {
        super(nodeId, partitionId, sorted2Original, original2Sorted);
        this.values = values;
    }

    protected ADBEntityEntry createForIndex(int id, int i) {
        return new ADBEntityCharEntry(id, this.values[i]);
    }
}
