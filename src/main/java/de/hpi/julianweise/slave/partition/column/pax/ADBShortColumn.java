package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBShortColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityShortEntry;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBShortColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements ShortComparator {
        @Override
        public int compare(short a, short b) {
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
        short[] original = new short[sortedIndices.length];
        int currentPointer = 0;
        for (short sortedIndex : this.sortedIndices) {
            if (min.getValueField().getShort(min) > this.values.getShort(sortedIndex)) continue;
            if (max.getValueField().getShort(max) < this.values.getShort(sortedIndex)) break;
            sorted[currentPointer] = this.values.getShort(sortedIndex);
            original[currentPointer++] = sortedIndex;
        }
        return new ADBShortColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(original, 0, currentPointer));
    }

    public int size() {
        return this.values.size();
    }

    protected ShortComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityShortEntry(id, this.values.getShort(index));
    }
}
