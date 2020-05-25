package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ADBPartitionHeaderFactory {

    public static ADBPartitionHeader createDefault(ObjectList<ADBEntity> data, int id) {
        if (data.size() < 1) {
            return new ADBPartitionHeader(new HashMap<>(), new HashMap<>(), data, id);
        }
        int numberOfFields = ADBEntityFactoryProvider.getInstance().getTargetClass().getDeclaredFields().length;
        Map<String, Comparable<Object>> minValues = new HashMap<>(numberOfFields);
        Map<String, Comparable<Object>> maxValues = new HashMap<>(numberOfFields);
        ADBPartitionHeaderFactory.getMinValuesPerField(data, minValues);
        ADBPartitionHeaderFactory.getMaxValuesPerField(data, maxValues);
        return new ADBPartitionHeader(minValues, maxValues, data, id);
    }

    private static void getMinValuesPerField(ObjectList<ADBEntity> data, Map<String, Comparable<Object>> minValues) {
        for (Field field : data.get(0).getClass().getDeclaredFields()) {
            Function<ADBEntity, Comparable<Object>> fieldGetter = data.get(0).getGetterForField(field.getName());
            for (ADBEntity datum : data) {
                if (!minValues.containsKey(field.getName())) {
                    minValues.put(field.getName(), fieldGetter.apply(datum));
                    continue;
                }
                if (fieldGetter.apply(datum).compareTo(minValues.get(field.getName())) < 0) {
                    minValues.put(field.getName(), fieldGetter.apply(datum));
                }
            }
        }
    }

    private static void getMaxValuesPerField(ObjectList<ADBEntity> data, Map<String, Comparable<Object>> maxValues) {
        for (Field field : data.get(0).getClass().getDeclaredFields()) {
            Function<ADBEntity, Comparable<Object>> fieldGetter = data.get(0).getGetterForField(field.getName());
            for (ADBEntity datum : data) {
                if (!maxValues.containsKey(field.getName())) {
                    maxValues.put(field.getName(), fieldGetter.apply(datum));
                    continue;
                }
                if (fieldGetter.apply(datum).compareTo(maxValues.get(field.getName())) > 0) {
                    maxValues.put(field.getName(), fieldGetter.apply(datum));
                }
            }
        }
    }

}
