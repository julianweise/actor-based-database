package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.lang.reflect.Field;

@AllArgsConstructor
@Getter
public class ADBSortedEntityAttributes {

    private final Field field;
    private final int[] sortedIndices;

    public ObjectList<ADBEntityEntry> getMaterialized(ObjectList<ADBEntity> data) {
        if (data.size() < 1) {
            return new ObjectArrayList<>();
        }

        ObjectList<ADBEntityEntry> materialized = new ObjectArrayList<>(this.sortedIndices.length);
        val factoryFunction = ADBEntityEntryFactory.typedEntries.get(field.getType());
        for(int sortedIndex : this.sortedIndices) {
            materialized.add(factoryFunction.apply(data.get(sortedIndex), field));
        }
        return materialized;
    }

    public int getLastIndex() {
        return sortedIndices[sortedIndices.length - 1];
    }

    public int getFirstIndex() {
        return sortedIndices[0];
    }
}
