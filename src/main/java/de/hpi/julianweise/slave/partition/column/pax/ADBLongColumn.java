package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBLongColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityLongEntry;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBLongColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements IntComparator {
        @Override
        public int compare(int a, int b) {
            return Long.compare(values.getLong(a), values.getLong(b));
        }
    }

    private final LongArrayList values = new LongArrayList();

    public ADBLongColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getLong(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setLong(entity, this.values.getLong(index));
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
        long[] sorted = new long[sortedIndices.length];
        int[] original = new int[sortedIndices.length];
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (min.getValueField().getLong(min) > this.values.getLong(sortedIndex)) continue;
            if (max.getValueField().getLong(max) < this.values.getLong(sortedIndex)) break;
            sorted[currentPointer] = this.values.getLong(sortedIndex);
            original[currentPointer++] = sortedIndex;
        }
        return new ADBLongColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(original, 0, currentPointer));
    }

    public int size() {
        return this.values.size();
    }

    protected IntComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityLongEntry(id, this.values.getLong(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getLong(index) == constant.getValueField().getLong(constant);
    }
}
