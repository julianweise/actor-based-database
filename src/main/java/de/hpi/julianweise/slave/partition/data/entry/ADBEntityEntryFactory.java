package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings("unused")
public abstract class ADBEntityEntryFactory {

    public static final Map<Class<?>, BiFunction<ADBEntity, Field, ADBEntityEntry>> typedEntries = new HashMap<Class<?>,
            BiFunction<ADBEntity, Field, ADBEntityEntry>>() {{
        put(byte.class, (ADBEntity entity, Field field) -> new ADBEntityBooleanEntry(entity.getInternalID(), field,
                entity));
        put(boolean.class, (ADBEntity entity, Field field) -> new ADBEntityBooleanEntry(entity.getInternalID(), field,
                entity));
        put(short.class, (ADBEntity entity, Field field) -> new ADBEntityShortEntry(entity.getInternalID(), field,
                entity));
        put(int.class, (ADBEntity entity, Field field) -> new ADBEntityIntEntry(entity.getInternalID(), field, entity));
        put(long.class, (ADBEntity entity, Field field) -> new ADBEntityLongEntry(entity.getInternalID(), field,
                entity));
        put(float.class, (ADBEntity entity, Field field) -> new ADBEntityFloatEntry(entity.getInternalID(), field,
                entity));
        put(double.class, (ADBEntity entity, Field field) -> new ADBEntityDoubleEntry(entity.getInternalID(), field,
                entity));
        put(String.class, (ADBEntity entity, Field field) -> new ADBEntityStringEntry(entity.getInternalID(), field,
                entity));
    }};

    @SneakyThrows
    public static ADBEntityEntry of(ADBEntity entity, Field field) {
        return ADBEntityEntryFactory.typedEntries.get(field.getType()).apply(entity, field);
    }
}
