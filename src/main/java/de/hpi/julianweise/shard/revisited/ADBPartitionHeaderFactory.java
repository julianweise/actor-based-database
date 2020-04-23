package de.hpi.julianweise.shard.revisited;

import de.hpi.julianweise.domain.ADBEntity;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import org.agrona.collections.Object2ObjectHashMap;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ADBPartitionHeaderFactory {

    public static ADBPartitionHeader createDefault(List<ADBEntity> data) {
        if (data.size() < 1) {
            return new ADBPartitionHeader(Object2ObjectMaps.emptyMap(), Object2ObjectMaps.emptyMap(), data);
        }
        Map<String, Comparable<Object>> minValues = new Object2ObjectHashMap<>();
        Map<String, Comparable<Object>> maxValues = new Object2ObjectHashMap<>();
        ADBPartitionHeaderFactory.getMinValuesPerField(data, minValues);
        ADBPartitionHeaderFactory.getMaxValuesPerField(data, maxValues);
        return new ADBPartitionHeader(minValues, maxValues, data);
    }

    private static void getMinValuesPerField(List<ADBEntity> data, Map<String, Comparable<Object>> minValues) {
        ADBPartitionHeaderFactory.getValuesPerField(data, -1, minValues);
    }

    private static void getMaxValuesPerField(List<ADBEntity> data, Map<String, Comparable<Object>> maxValues) {
        ADBPartitionHeaderFactory.getValuesPerField(data, +1, maxValues);
    }

    private static void getValuesPerField(List<ADBEntity> data, int comparisonValue, Map<String, Comparable<Object>> collection) {
        for (Field field : data.get(0).getClass().getDeclaredFields()) {
            Function<ADBEntity, Comparable<Object>> fieldGetter = data.get(0).getGetterForField(field.getName());
            for (ADBEntity datum : data) {
                if (!collection.containsKey(field.getName())) {
                    collection.put(field.getName(), fieldGetter.apply(datum));
                    continue;
                }
                if (fieldGetter.apply(datum).compareTo(collection.get(field.getName())) == comparisonValue) {
                    collection.put(field.getName(), fieldGetter.apply(datum));
                }
            }
        }
    }
}
