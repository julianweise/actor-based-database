package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBIntColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBIntColumn extends ADBColumn {

    private final IntArrayList values = new IntArrayList();

    public ADBIntColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getInt(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setInt(entity, this.values.getInt(index));
        return entity;
    }

    @Override
    public ADBColumn complete() {
        this.values.trim();
        return super.complete();
    }

    public int size() {
        return this.values.size();
    }

    @SneakyThrows
    @Override
    public ADBColumnSorted getSortedColumn(ADBEntityEntry min, ADBEntityEntry max) {
        int[] sorted = new int[sortedIndices.length];
        int[] sortedToOriginal = new int[sortedIndices.length];
        int[] originalToSorted = new int[sortedIndices.length];
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (min.getValueField().getInt(min) > this.values.getInt(sortedIndex)) continue;
            if (max.getValueField().getInt(max) < this.values.getInt(sortedIndex)) break;
            sorted[currentPointer] = this.values.getInt(sortedIndex);
            sortedToOriginal[currentPointer] = sortedIndex;
            originalToSorted[sortedIndex] = currentPointer++;
        }
        return new ADBIntColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(sortedToOriginal, 0, currentPointer), originalToSorted);
    }

    protected IntComparator getIndexedValueComparator() {
        return (a, b) -> Integer.compare(values.getInt(a), values.getInt(b));
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityIntEntry(id, this.values.getInt(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getInt(index) == constant.getValueField().getInt(constant);
    }
}
