package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.reflect.Field;

public abstract class ADBColumn {

    protected final int partitionId;
    protected transient int[] sortedIndices;
    protected transient Field entityField;
    protected transient boolean finalized = false;

    public ADBColumn(Field entityField, int partitionId) {
        this.entityField = entityField;
        this.partitionId = partitionId;
    }

    public abstract void add(ADBEntity entity);
    public abstract ADBEntity setField(ADBEntity entity, int index);

    public ADBColumn complete() {
        this.finalized = true;
        this.sortedIndices = this.calculateSortedIndices();
        return this;
    }

    protected abstract ADBEntityEntry getEntry(int id, int index);

    protected abstract IntComparator getIndexedValueComparator();

    public abstract int size();

    protected int[] calculateSortedIndices() {
        int[] sortedIndices = new int[this.size()];
        for (int i = 0; i < sortedIndices.length; i++) sortedIndices[i] = i;
        IntArrays.parallelQuickSort(sortedIndices, this.getIndexedValueComparator());
        return sortedIndices;
    }

    public ADBEntityEntry getSmallest() {
        int id = ADBInternalIDHelper.createID(ADBSlave.ID, this.partitionId, this.sortedIndices[0]);
        return this.getEntry(id, this.sortedIndices[0]);
    }

    public ADBEntityEntry getLargest() {
        int id = ADBInternalIDHelper.createID(ADBSlave.ID, partitionId, sortedIndices[sortedIndices.length - 1]);
        return this.getEntry(id, this.sortedIndices[this.sortedIndices.length - 1]);
    }

    public abstract boolean satisfy(int index, ADBPredicateConstant constant);

    public abstract ADBColumnSorted getSortedColumn(ADBEntityEntry min, ADBEntityEntry max);
}
