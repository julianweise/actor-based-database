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
    protected int[] sortedToOriginal;
    protected int[] originalToSorted;

    public ObjectList<ADBEntityEntry> materializeSorted() {
        ObjectList<ADBEntityEntry> result = new ObjectArrayList<>(this.sortedToOriginal.length);
        for (int i = 0; i < this.sortedToOriginal.length; i++) {
            int id = ADBInternalIDHelper.createID(this.nodeId, this.partitionId, this.sortedToOriginal[i]);
            result.add(this.createForIndex(id, i));
        }
        return result;
    }

    public ObjectList<ADBEntityEntry> materializeOriginal() {
        ADBEntityEntry[] original = new ADBEntityIntEntry[this.sortedToOriginal.length];
        for (int i = 0; i < this.sortedToOriginal.length; i++) {
            int id = ADBInternalIDHelper.createID(this.nodeId, this.partitionId, this.sortedToOriginal[i]);
            original[this.sortedToOriginal[i]] = this.createForIndex(id, i);
        }
        return ObjectArrayList.wrap(original);
    }

    public ADBEntityEntry getByOriginalIndex(int originalIndex) {
        int id = ADBInternalIDHelper.createID(this.nodeId, this.partitionId, originalIndex);
        if (this.originalToSorted[originalIndex] >= this.size() || this.originalToSorted[originalIndex] == -1) {
            throw new IllegalArgumentException("Original Index has been pruned");
        }
        return this.createForIndex(id, this.originalToSorted[originalIndex]);
    }

    public int size() {
        return this.sortedToOriginal.length;
    }

    protected abstract ADBEntityEntry createForIndex(int id, int index);
}
