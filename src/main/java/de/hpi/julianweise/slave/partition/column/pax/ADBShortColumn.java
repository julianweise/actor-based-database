package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBShortColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityShortEntry;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBShortColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements IntComparator {
        @Override
        public int compare(int a, int b) {
            return Short.compare(values.getShort(a), values.getShort(b));
        }
    }

    private final ShortArrayList values = new ShortArrayList();

    public ADBShortColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getShort(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setShort(entity, this.values.getShort(index));
        return entity;
    }

    @Override
    public ADBColumn complete() {
        this.values.trim();
        return super.complete();
    }

    @SneakyThrows
    @Override
    public ADBColumnSorted getSortedColumn(ADBEntityEntry min, ADBEntityEntry max) {
        short[] sorted = new short[sortedIndices.length];
        int[] sortedToOriginal = new int[sortedIndices.length];
        int[] originalToSorted = new int[sortedIndices.length];
        Arrays.fill(originalToSorted, -1);
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (min.getValueField().getShort(min) > this.values.getShort(sortedIndex)) continue;
            if (max.getValueField().getShort(max) < this.values.getShort(sortedIndex)) break;
            sorted[currentPointer] = this.values.getShort(sortedIndex);
            sortedToOriginal[currentPointer] = sortedIndex;
            originalToSorted[sortedIndex] = currentPointer++;
        }
        return new ADBShortColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(sortedToOriginal, 0, currentPointer), originalToSorted);
    }

    public int size() {
        return this.values.size();
    }

    protected IntComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityShortEntry(id, this.values.getShort(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getShort(index) == constant.getValueField().getShort(constant);
    }
}
