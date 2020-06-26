package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBBooleanColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityBooleanEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBBooleanColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements ShortComparator {
        @Override
        public int compare(short a, short b) {
            return Boolean.compare(values.getBoolean(a), values.getBoolean(b));
        }
    }

    private final BooleanArrayList values = new BooleanArrayList();

    public ADBBooleanColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getBoolean(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setBoolean(entity, this.values.getBoolean(index));
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
        boolean[] sorted = new boolean[sortedIndices.length];
        short[] original = new short[sortedIndices.length];
        int currentPointer = 0;
        for (short sortedIndex : this.sortedIndices) {
            sorted[currentPointer] = this.values.getBoolean(sortedIndex);
            original[currentPointer++] = sortedIndex;
        }
        return new ADBBooleanColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(original, 0, currentPointer));
    }

    public int size() {
        return this.values.size();
    }

    protected ShortComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityBooleanEntry(id, this.values.getBoolean(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.getBoolean(index) == constant.getValueField().getBoolean(constant);
    }
}
