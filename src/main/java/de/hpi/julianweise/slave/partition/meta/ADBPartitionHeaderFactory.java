package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import lombok.val;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class ADBPartitionHeaderFactory {

    public static ADBPartitionHeader createDefault(ObjectList<ADBEntity> data, int id,
                                                   Map<String, ADBSortedEntityAttributes> sortedAttributes) {
        if (data.size() < 1) {
            return new ADBPartitionHeader(new HashMap<>(), new HashMap<>(), data, id);
        }
        val minValues = ADBPartitionHeaderFactory.getMinValuesPerField(data, sortedAttributes);
        val maxValues = ADBPartitionHeaderFactory.getMaxValuesPerField(data, sortedAttributes);
        return new ADBPartitionHeader(minValues, maxValues, data, id);
    }

    @SneakyThrows
    private static Map<String, ADBEntityEntry> getMinValuesPerField(ObjectList<ADBEntity> data,
                                                                    Map<String, ADBSortedEntityAttributes> sortedAttr) {
        Map<String, ADBEntityEntry> minValues = new HashMap<>(sortedAttr.size());
        for (Map.Entry<String, ADBSortedEntityAttributes> entry : sortedAttr.entrySet()) {
            Field field = data.get(0).getClass().getDeclaredField(entry.getKey());
            ADBSortedEntityAttributes lSortedAttributes = entry.getValue();
            minValues.put(entry.getKey(), ADBEntityEntryFactory.of(data.get(lSortedAttributes.getFirstIndex()),field));
        }
        return minValues;
    }

    @SneakyThrows
    private static Map<String, ADBEntityEntry> getMaxValuesPerField(ObjectList<ADBEntity> data,
                                                                                  Map<String, ADBSortedEntityAttributes> sortedAttr) {
        Map<String, ADBEntityEntry> minValues = new HashMap<>(sortedAttr.size());
        for (Map.Entry<String, ADBSortedEntityAttributes> entry : sortedAttr.entrySet()) {
            Field field = data.get(0).getClass().getDeclaredField(entry.getKey());
            ADBSortedEntityAttributes lSortedAttributes = entry.getValue();
            minValues.put(entry.getKey(), ADBEntityEntryFactory.of(data.get(lSortedAttributes.getLastIndex()),field));
        }
        return minValues;
    }

}
