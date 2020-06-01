package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = boolean.class)
public class ADBEntityBooleanEntry extends ADBEntityEntry {

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityBooleanEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }
    public boolean value;

    public ADBEntityBooleanEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        try {
            this.value = field.getBoolean(entity);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityBooleanEntry.valueField;
    }
}
