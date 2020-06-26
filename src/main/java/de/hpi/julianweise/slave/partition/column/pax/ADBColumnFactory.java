package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.agrona.collections.Object2ObjectHashMap;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ADBColumnFactory {

    public static final HashMap<Class<?>, BiFunction<Field, Integer, ADBColumn>> typedColumns =
            new HashMap<Class<?>, BiFunction<Field, Integer, ADBColumn>>() {{
        put(byte.class, ADBByteColumn::new);
        put(boolean.class, ADBBooleanColumn::new);
        put(short.class, ADBShortColumn::new);
        put(int.class, ADBIntColumn::new);
        put(long.class, ADBLongColumn::new);
        put(float.class, ADBFloatColumn::new);
        put(double.class, ADBDoubleColumn::new);
        put(String.class, ADBStringColumn::new);
    }};
    
    public static ADBColumn createDefault(Field field, int partitionId) {
        return ADBColumnFactory.typedColumns.get(field.getType()).apply(field, partitionId);
    }

    public static Map<String, ADBColumn> createDefault(ObjectList<ADBEntity> data, int partitionId) {
        if (data.size() < 1) {
            return new Object2ObjectHashMap<>();
        }
        Object2ObjectOpenHashMap<String, ADBColumn> result = new Object2ObjectOpenHashMap<>();
        for (Field field : data.get(0).getClass().getDeclaredFields()) {
            ADBColumn newColumn = ADBColumnFactory.createDefault(field, partitionId);
            data.forEach(newColumn::add);
            result.put(field.getName(), newColumn.complete());
        }
        return result;
    }
}
