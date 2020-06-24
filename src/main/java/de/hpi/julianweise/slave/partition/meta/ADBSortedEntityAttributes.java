package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

@AllArgsConstructor
@Getter
public class ADBSortedEntityAttributes {

    private final static Logger LOG = LoggerFactory.getLogger(ADBSortedEntityAttributes.class);

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

    public ObjectList<ADBEntityEntry> getMaterialized(ObjectList<ADBEntity> data, ADBEntityEntry min, ADBEntityEntry max) {
        if (data.size() < 1) {
            return new ObjectArrayList<>();
        }

        ObjectList<ADBEntityEntry> materialized = new ObjectArrayList<>(this.sortedIndices.length);
        val factoryFunction = ADBEntityEntryFactory.typedEntries.get(field.getType());
        ADBComparator minComparator = ADBComparator.getFor(min.getValueField(), min.getValueField());
        ADBComparator maxComparator = ADBComparator.getFor(max.getValueField(), max.getValueField());
        int startIndex = 0;
        while (startIndex + 1 < sortedIndices.length && minComparator.compare(min,
                factoryFunction.apply(data.get(sortedIndices[startIndex + 1]), field)) > 0) startIndex++;
        for (int a = startIndex; a < this.sortedIndices.length; a++) {
            ADBEntityEntry entry = factoryFunction.apply(data.get(this.sortedIndices[a]), field);
            if (maxComparator.compare(max, entry) < 0) {
                LOG.debug("Min/Max strategy reduced by attr {}%", (1-materialized.size() / sortedIndices.length) * 100);
                return materialized;
            }
            materialized.add(entry);
        }

        LOG.info("Min/Max strategy reduced attr by {}%", (1 - materialized.size() / sortedIndices.length) * 100);
        return materialized;
    }

    public int getLastIndex() {
        return sortedIndices[sortedIndices.length - 1];
    }

    public int getFirstIndex() {
        return sortedIndices[0];
    }
}
