package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBDoubleColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityDoubleEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBDoubleColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements IntComparator {
        @Override
        public int compare(int a, int b) {
            return Double.compare(values.getDouble(a), values.getDouble(b));
        }
    }

    private final DoubleArrayList values = new DoubleArrayList();

    public ADBDoubleColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getDouble(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setDouble(entity, this.values.getDouble(index));
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
        double[] sorted = new double[sortedIndices.length];
        int[] sortedToOriginal = new int[sortedIndices.length];
        int[] originalToSorted = new int[sortedIndices.length];
        Arrays.fill(originalToSorted, -1);
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (min.getValueField().getDouble(min) > this.values.getDouble(sortedIndex)) continue;
            if (max.getValueField().getDouble(max) < this.values.getDouble(sortedIndex)) break;
            sorted[currentPointer] = this.values.getDouble(sortedIndex);
            sortedToOriginal[currentPointer] = sortedIndex;
            originalToSorted[sortedIndex] = currentPointer++;

        }
        return new ADBDoubleColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(sortedToOriginal, 0, currentPointer), originalToSorted);
    }

    public int size() {
        return this.values.size();
    }

    protected IntComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityDoubleEntry(id, this.values.getDouble(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getDouble(index) == constant.getValueField().getDouble(constant);
    }
}
