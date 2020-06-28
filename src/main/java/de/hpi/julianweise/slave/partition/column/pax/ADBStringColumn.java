package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.query.selection.constant.ADBPredicateConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBStringColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityStringEntry;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ADBStringColumn extends ADBColumn {

    @AllArgsConstructor
    private class ValuesIndexComparator implements IntComparator {
        @Override
        public int compare(int a, int b) {
            return values.get(a).compareTo(values.get(b));
        }
    }

    private final ObjectArrayList<String> values = new ObjectArrayList<>();

    public ADBStringColumn(Field field, int partitionId) {
        super(field, partitionId);
    }

    @Override
    @SneakyThrows
    public void add(ADBEntity entity) {
        assert !this.finalized : "Tried to add data to a finalized column";
        this.values.add((String) this.entityField.get(entity));
    }

    @SneakyThrows
    public ADBEntity setField(ADBEntity entity, int index) {
        this.entityField.set(entity, this.values.get(index));
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
        String[] sorted = new String[sortedIndices.length];
        int[] original = new int[sortedIndices.length];
        int currentPointer = 0;
        for (int sortedIndex : this.sortedIndices) {
            if (((String) min.getValueField().get(min)).compareTo(this.values.get(sortedIndex)) > 0) continue;
            if (((String) max.getValueField().get(max)).compareTo(this.values.get(sortedIndex)) < 0) break;
            sorted[currentPointer] = this.values.get(sortedIndex);
            original[currentPointer++] = sortedIndex;
        }
        return new ADBStringColumnSorted(ADBSlave.ID, partitionId, Arrays.copyOfRange(sorted, 0, currentPointer),
                Arrays.copyOfRange(original, 0, currentPointer));
    }

    public int size() {
        return this.values.size();
    }

    protected IntComparator getIndexedValueComparator() {
        return new ValuesIndexComparator();
    }

    protected ADBEntityEntry getEntry(int id, int index) {
        return new ADBEntityStringEntry(id, this.values.get(index));
    }

    @SneakyThrows
    public boolean satisfy(int index, ADBPredicateConstant constant) {
        return this.values.get(index).equals(constant.getValueField().get(constant));
    }
}
