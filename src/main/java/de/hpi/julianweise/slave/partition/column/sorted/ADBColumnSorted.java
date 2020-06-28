package de.hpi.julianweise.slave.partition.column.sorted;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public abstract class ADBColumnSorted {

    protected int nodeId;
    protected int partitionId;
    protected int[] originalIndices;

    public ObjectList<ADBEntityEntry> materializeSorted() {
        ObjectList<ADBEntityEntry> result = new ObjectArrayList<>(this.originalIndices.length);
        for (int i = 0; i < this.originalIndices.length; i++) {
            int id = ADBInternalIDHelper.createID(this.nodeId, this.partitionId, this.originalIndices[i]);
            result.add(this.createForIndex(id, i));
        }
        return result;
    }

    public ObjectList<ADBEntityEntry> materializeOriginal() {
        ADBEntityEntry[] original = new ADBEntityIntEntry[this.originalIndices.length];
        for (int i = 0; i < this.originalIndices.length; i++) {
            int id = ADBInternalIDHelper.createID(this.nodeId, this.partitionId, this.originalIndices[i]);
            original[this.originalIndices[i]] = this.createForIndex(id, i);
        }
        return ObjectArrayList.wrap(original);
    }

    public int size() {
        return this.originalIndices.length;
    }

    protected abstract ADBEntityEntry createForIndex(int id, int index);
}
