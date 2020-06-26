package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBByteColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityByteEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBByteColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements ShortComparator {
        @Override
        public int compare(short a, short b) {
            return Byte.compare(values.getByte(a), values.getByte(b));
        }
    }

    private final ByteArrayList values = new ByteArrayList();

    public ADBByteColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add(this.entityField.getByte(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.setByte(entity, this.values.getByte(index));
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
        byte[] sorted = new byte[sortedIndices.length];
        short[] original = new short[sortedIndices.length];
        int currentPointer = 0;
        for (short sortedIndex : this.sortedIndices) {
            if (min.getValueField().getByte(min) > this.values.getByte(sortedIndex)) continue;
            if (max.getValueField().getByte(max) < this.values.getByte(sortedIndex)) break;
            sorted[currentPointer] = this.values.getByte(sortedIndex);
            original[currentPointer++] = sortedIndex;
        }
        return new ADBByteColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(original, 0, currentPointer));
    }

    public int size() {
        return this.values.size();
    }

    protected ShortComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityByteEntry(id, this.values.getByte(index));
    }
}
