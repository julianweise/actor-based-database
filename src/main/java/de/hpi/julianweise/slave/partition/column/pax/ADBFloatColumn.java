package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBFloatColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityFloatEntry;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBFloatColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements IntComparator {
        @Override
        public int compare(int a, int b) {
            return Float.compare(values.getFloat(a), values.getFloat(b));
        }
    }

    private final FloatArrayList values = new FloatArrayList();

    public ADBFloatColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getFloat(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setFloat(entity, this.values.getFloat(index));
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
        float[] sorted = new float[sortedIndices.length];
        int[] sortedToOriginal = new int[sortedIndices.length];
        int[] originalToSorted = new int[sortedIndices.length];
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (min.getValueField().getFloat(min) > this.values.getFloat(sortedIndex)) continue;
            if (max.getValueField().getFloat(max) < this.values.getFloat(sortedIndex)) break;
            sorted[currentPointer] = this.values.getFloat(sortedIndex);
            sortedToOriginal[currentPointer] = sortedIndex;
            originalToSorted[sortedIndex] = currentPointer++;

        }
        return new ADBFloatColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(sortedToOriginal, 0, currentPointer), originalToSorted);
    }

    public int size() {
        return this.values.size();
    }

    protected IntComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityFloatEntry(id, this.values.getFloat(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getFloat(index) == constant.getValueField().getFloat(constant);
    }
}
